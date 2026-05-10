<?php
function cisco_settings_h($value) {
    return htmlspecialchars((string)$value, ENT_QUOTES, 'UTF-8');
}

function cisco_settings_truthy($value) {
    return in_array(strtolower(trim((string)$value)), ['1', 'true', 'yes', 'on'], true);
}

function cisco_settings_ensure_table($pdo) {
    $pdo->exec("CREATE TABLE IF NOT EXISTS `endpoints-modulesettings-cisco` (`parameter` VARCHAR(128) NOT NULL, `value` TEXT, PRIMARY KEY (`parameter`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci");
    try {
        $pdo->exec("ALTER TABLE `endpoints-modulesettings-cisco` MODIFY `parameter` VARCHAR(128) NOT NULL");
    } catch (Throwable $exc) {
    }
    try {
        $pdo->exec("ALTER TABLE `endpoints-modulesettings-cisco` ADD PRIMARY KEY (`parameter`)");
    } catch (Throwable $exc) {
    }
}

function cisco_settings_defaults() {
    return [
        'messageinfo-enabled' => '1',
        'messageinfo-showsender' => '1',
        'messageinfo-productname' => '1',
        'ucmsync' => '0',
        'ucmsync-ip' => '',
        'ucmsync-username' => '',
        'ucmsync-password' => '',
        'ucmsync-interval' => '300',
        'authrelay' => 'false',
    ];
}

function cisco_settings_load($pdo) {
    cisco_settings_ensure_table($pdo);
    $values = cisco_settings_defaults();
    $stmt = $pdo->query("SELECT `parameter`, `value` FROM `endpoints-modulesettings-cisco`");
    foreach ($stmt->fetchAll(PDO::FETCH_ASSOC) as $row) {
        $key = (string)($row['parameter'] ?? '');
        if (array_key_exists($key, $values)) {
            $values[$key] = (string)($row['value'] ?? '');
        }
    }
    cisco_settings_save($pdo, $values);
    return $values;
}

function cisco_settings_save($pdo, $values) {
    cisco_settings_ensure_table($pdo);
    $pdo->beginTransaction();
    try {
        $pdo->exec("DELETE FROM `endpoints-modulesettings-cisco`");
        $insert = $pdo->prepare("INSERT INTO `endpoints-modulesettings-cisco` (`parameter`, `value`) VALUES (:parameter, :value)");
        foreach ($values as $parameter => $value) {
            $insert->execute(['parameter' => $parameter, 'value' => $value]);
        }
        $pdo->commit();
    } catch (Throwable $exc) {
        if ($pdo->inTransaction()) {
            $pdo->rollBack();
        }
        throw $exc;
    }
}

$message = '';
$error = '';
$values = cisco_settings_load($pdo);

if ($_SERVER['REQUEST_METHOD'] === 'POST') {
    try {
        $authrelayEnabled = isset($_POST['authrelay-enabled']);
        $messageInfoEnabled = isset($_POST['messageinfo-enabled']);
        $values = [
            'messageinfo-enabled' => $messageInfoEnabled ? '1' : '0',
            'messageinfo-showsender' => $messageInfoEnabled && isset($_POST['messageinfo-showsender']) ? '1' : '0',
            'messageinfo-productname' => $messageInfoEnabled && isset($_POST['messageinfo-productname']) ? '1' : '0',
            'ucmsync' => isset($_POST['ucmsync']) ? '1' : '0',
            'ucmsync-ip' => trim((string)($_POST['ucmsync-ip'] ?? '')),
            'ucmsync-username' => trim((string)($_POST['ucmsync-username'] ?? '')),
            'ucmsync-password' => (string)($_POST['ucmsync-password'] ?? ''),
            'ucmsync-interval' => trim((string)($_POST['ucmsync-interval'] ?? '300')),
            'authrelay' => $authrelayEnabled ? trim((string)($_POST['authrelay'] ?? '')) : 'false',
        ];
        if ($values['ucmsync-interval'] === '' || !ctype_digit($values['ucmsync-interval'])) {
            $values['ucmsync-interval'] = '300';
        }
        if ($authrelayEnabled && $values['authrelay'] === '') {
            throw new RuntimeException('Auth Relay URL is required when Auth Relay is enabled.');
        }
        cisco_settings_save($pdo, $values);
        $message = 'Cisco module settings saved.';
    } catch (Throwable $exc) {
        $error = $exc->getMessage();
    }
}

$authrelayEnabled = trim((string)$values['authrelay']) !== '' && strtolower(trim((string)$values['authrelay'])) !== 'false';
?>
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
body{font-family:Tahoma,sans-serif;margin:0;padding:18px;color:#202124;background:#fff}.grid{display:grid;gap:12px}.section{border-top:1px solid #eee;padding-top:12px;margin-top:4px}.nested{display:grid;gap:12px}.row{display:grid;gap:6px}.check{display:flex;align-items:center;gap:8px;font-weight:400}.control{padding:10px;border:1px solid #ddd;border-radius:4px;font:inherit;box-sizing:border-box;width:100%}.button{background:#1976D2;color:#fff;border:0;border-radius:4px;padding:10px 14px;font:inherit;cursor:pointer;justify-self:start}.success{background:#E8F5E9;border:1px solid #A5D6A7;color:#1B5E20;padding:10px;border-radius:6px;margin-bottom:12px}.error{background:#FFEBEE;border:1px solid #EF9A9A;color:#B71C1C;padding:10px;border-radius:6px;margin-bottom:12px}.hidden{display:none!important}@media(prefers-color-scheme:dark){body{background:#1e1e1e;color:#e0e0e0}.section{border-top-color:#333}.control{background:#171717;border-color:#333;color:#eee}.button{background:#BB86FC;color:#000}.success{background:#14351A;border-color:#2E7D32;color:#C8E6C9}.error{background:#3B1515;border-color:#6D2A2A;color:#FFCDD2}}
</style>
</head>
<body>
<?php if ($message): ?><div class="success"><?= cisco_settings_h($message) ?></div><?php endif; ?>
<?php if ($error): ?><div class="error"><?= cisco_settings_h($error) ?></div><?php endif; ?>
<form method="post" class="grid">
    <label class="check"><input type="checkbox" id="messageInfoToggle" name="messageinfo-enabled" value="1" <?= cisco_settings_truthy($values['messageinfo-enabled']) ? 'checked' : '' ?>> Message Info Enabled</label>
    <div class="nested" id="messageInfoSettings">
        <label class="check"><input type="checkbox" name="messageinfo-showsender" value="1" <?= cisco_settings_truthy($values['messageinfo-showsender']) ? 'checked' : '' ?>> Show Sender on Info Page</label>
        <label class="check"><input type="checkbox" name="messageinfo-productname" value="1" <?= cisco_settings_truthy($values['messageinfo-productname']) ? 'checked' : '' ?>> Show Product Name on Info Page</label>
    </div>

    <div class="section grid">
        <label class="check"><input type="checkbox" id="ucmSyncToggle" name="ucmsync" value="1" <?= cisco_settings_truthy($values['ucmsync']) ? 'checked' : '' ?>> Sync with Unified Communications Manager (CUCM)</label>
        <div class="nested" id="ucmSyncSettings">
			<small class="note">Currently, only CUCM 11 or later is supported. Support for older versions of AXL is planned.</small>
            <div class="row"><label>CUCM Server(s)</label><input class="control" name="ucmsync-ip" value="<?= cisco_settings_h($values['ucmsync-ip']) ?>">
			<small class="note">Enter the IP of the Publisher. If you have any Subcribers, you can them add them separated by commas for redundancy. All servers must have the Cisco AXL Web Service activated.</small>
			</div>
            <div class="row"><label>Application User ID</label><input class="control" name="ucmsync-username" value="<?= cisco_settings_h($values['ucmsync-username']) ?>"></div>
            <div class="row"><label>Application User Password</label><input class="control" type="password" name="ucmsync-password" value="<?= cisco_settings_h($values['ucmsync-password']) ?>" autocomplete="new-password"></div>
            <div class="row"><label>CUCM Sync Interval</label><input class="control" type="number" min="1" name="ucmsync-interval" value="<?= cisco_settings_h($values['ucmsync-interval']) ?>"></div>
        </div>
    </div>

    <div class="section grid">
        <label class="check"><input type="checkbox" id="authRelayToggle" name="authrelay-enabled" value="1" <?= $authrelayEnabled ? 'checked' : '' ?>> Auth Relay</label>
        <div class="nested" id="authRelaySettings">
            <div class="row"><label>Auth Relay URL</label><input class="control" type="url" name="authrelay" value="<?= cisco_settings_h($authrelayEnabled ? $values['authrelay'] : '') ?>" placeholder="https://example.local/auth"></div>
        </div>
    </div>

    <button class="button" type="submit">Save Cisco Settings</button>
</form>
<script>
function bindToggle(toggleId, targetId) {
  const toggle = document.getElementById(toggleId);
  const target = document.getElementById(targetId);
  if (!toggle || !target) return;
  function sync() {
    target.classList.toggle('hidden', !toggle.checked);
  }
  toggle.addEventListener('change', sync);
  sync();
}
bindToggle('messageInfoToggle', 'messageInfoSettings');
bindToggle('ucmSyncToggle', 'ucmSyncSettings');
bindToggle('authRelayToggle', 'authRelaySettings');
</script>
</body>
</html>