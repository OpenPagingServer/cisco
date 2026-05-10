<?php
function ef_h($value) {
    return htmlspecialchars((string)$value, ENT_QUOTES, 'UTF-8');
}

function selected($actual, $expected) {
    return (string)$actual === (string)$expected ? 'selected' : '';
}

$message = '';
$error = '';
$models = ['7811', '7821', '7841', '7861', '7925', '7926', '7931', '7940', '7941', '7942', '7945', '7960', '7961', '7962', '7965', '7970', '7971', '7975', '8811', '8841', '8845', '8851', '8861', '8865'];
$audioModes = ['Multicast', 'Unicast', 'Disabled'];
$visualModes = ['None', 'Text', 'Image'];
$textOnlyModels = ['7811', '7821', '7841', '7861'];
$volumes = ['0', '10', '20', '30', '40', '50', '60', '70', '80', '90', '100', 'asis'];

$selectedModel = trim((string)($_POST['model'] ?? ''));
$activeVisualModes = in_array($selectedModel, $textOnlyModels, true) ? ['None', 'Text'] : $visualModes;

$values = [
    'macaddr' => '',
    'name' => '',
    'ipv4' => '',
    'unchecked' => '',
    'audio' => 'Multicast',
    'model' => $selectedModel,
    'visual' => in_array($selectedModel, $textOnlyModels, true) ? 'Text' : 'Image',
    'volume' => 'asis',
];

try {
    $pdo->exec("CREATE TABLE IF NOT EXISTS `endpoints-output-cisco` (
        `macaddr` VARCHAR(64) NOT NULL,
        `name` VARCHAR(255) DEFAULT '',
        `ipv4` VARCHAR(45) DEFAULT '',
        `status` ENUM('New', 'Unchecked', 'Offline', 'Online') NOT NULL DEFAULT 'Unchecked',
        `audio` ENUM('Multicast', 'Unicast', 'Disabled') NOT NULL DEFAULT 'Multicast',
        `model` ENUM('', '7811', '7821', '7841', '7861', '7925', '7926', '7931', '7940', '7941', '7942', '7945', '7960', '7961', '7962', '7965', '7970', '7971', '7975', '8811', '8841', '8845', '8851', '8861', '8865') NOT NULL DEFAULT '',
        `visual` ENUM('None', 'Text', 'Image') NOT NULL DEFAULT 'Image',
        `volume` ENUM('0', '10', '20', '30', '40', '50', '60', '70', '80', '90', '100', 'asis') NOT NULL DEFAULT 'asis',
        `addedby` ENUM('MANUAL', 'UCM') NOT NULL DEFAULT 'MANUAL',
        PRIMARY KEY (`macaddr`),
        KEY `ipv4_idx` (`ipv4`),
        KEY `status_idx` (`status`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci");

    if ($_SERVER['REQUEST_METHOD'] === 'POST' && isset($_POST['macaddr'])) {
        foreach ($values as $key => $_default) {
            $values[$key] = trim((string)($_POST[$key] ?? $_default));
        }

        $values['unchecked'] = isset($_POST['unchecked']) ? '1' : '';
        $values['macaddr'] = strtoupper(preg_replace('/[^A-Za-z0-9]/', '', $values['macaddr']));

        if ($values['macaddr'] !== '' && strpos($values['macaddr'], 'SEP') !== 0) {
            $values['macaddr'] = 'SEP' . $values['macaddr'];
        }

        if ($values['macaddr'] === '') {
            throw new RuntimeException('MAC/SEP identifier is required.');
        }

        if (!in_array($values['model'], $models, true)) {
            throw new RuntimeException('Phone model is required.');
        }

        $activeVisualModes = in_array($values['model'], $textOnlyModels, true) ? ['None', 'Text'] : $visualModes;

        $status = isset($_POST['unchecked']) ? 'Unchecked' : 'New';

        if (!in_array($values['audio'], $audioModes, true)) {
            $values['audio'] = 'Multicast';
        }

        if (!in_array($values['visual'], $activeVisualModes, true)) {
            $values['visual'] = in_array($values['model'], $textOnlyModels, true) ? 'Text' : 'Image';
        }

        if (!in_array($values['volume'], $volumes, true)) {
            $values['volume'] = 'asis';
        }

        $stmt = $pdo->prepare("SELECT COUNT(*) FROM `endpoints-output-cisco` WHERE `macaddr` = :macaddr");
        $stmt->execute(['macaddr' => $values['macaddr']]);

        if ((int)$stmt->fetchColumn() > 0) {
            throw new RuntimeException('That Cisco SEP endpoint already exists.');
        }

        $stmt = $pdo->prepare("INSERT INTO `endpoints-output-cisco` (`macaddr`, `name`, `ipv4`, `status`, `audio`, `model`, `visual`, `volume`, `addedby`) VALUES (:macaddr, :name, :ipv4, :status, :audio, :model, :visual, :volume, 'MANUAL')");
        $stmt->execute([
            'macaddr' => $values['macaddr'],
            'name' => $values['name'],
            'ipv4' => $values['ipv4'],
            'status' => $status,
            'audio' => $values['audio'],
            'model' => $values['model'],
            'visual' => $values['visual'],
            'volume' => $values['volume'],
        ]);

        $message = 'Cisco enterprise endpoint added.';
        $selectedModel = '';
        $activeVisualModes = $visualModes;
        $values = [
            'macaddr' => '',
            'name' => '',
            'ipv4' => '',
            'unchecked' => '',
            'audio' => 'Multicast',
            'model' => '',
            'visual' => 'Image',
            'volume' => 'asis',
        ];
    }
} catch (Throwable $exc) {
    $error = $exc->getMessage();
}

$showModelPicker = $selectedModel === '' || !in_array($selectedModel, $models, true);
?>
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
body{font-family:Tahoma,sans-serif;margin:0;padding:18px;color:#202124;background:#fff}.grid{display:grid;gap:12px}.row{display:grid;gap:6px}label{font-weight:500}.check{display:flex;align-items:center;gap:8px;font-weight:400}.control{padding:10px;border:1px solid #ddd;border-radius:4px;font:inherit}.button{background:#1976D2;color:#fff;border:0;border-radius:4px;padding:10px 14px;font:inherit;cursor:pointer;text-decoration:none;display:inline-flex;align-items:center;justify-content:center}.button.secondary{background:#5f6368}.success{background:#E8F5E9;border:1px solid #A5D6A7;color:#1B5E20;padding:10px;border-radius:6px;margin-bottom:12px}.error{background:#FFEBEE;border:1px solid #EF9A9A;color:#B71C1C;padding:10px;border-radius:6px;margin-bottom:12px}.model-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(92px,1fr));gap:10px}.title{font-size:22px;font-weight:600;margin:0 0 14px}.subtitle{margin:0 0 18px;color:#5f6368}.selected-model{background:#E3F2FD;border:1px solid #90CAF9;color:#0D47A1;padding:10px;border-radius:6px;margin-bottom:12px}.topbar{display:flex;align-items:center;gap:10px;margin-bottom:14px}.audio-key{font-size:13px;color:#5f6368;line-height:1.45;margin-top:2px}.audio-key div{margin:4px 0}.audio-key strong{color:#202124}@media(prefers-color-scheme:dark){body{background:#1e1e1e;color:#e0e0e0}.control{background:#171717;border-color:#333;color:#eee}.button{background:#BB86FC;color:#000}.button.secondary{background:#444;color:#eee}.subtitle{color:#aaa}.selected-model{background:#102334;border-color:#24577d;color:#b8ddff}.audio-key{color:#aaa}.audio-key strong{color:#e0e0e0}}
</style>
</head>
<body>
<?php if ($message): ?><div class="success"><?= ef_h($message) ?></div><?php endif; ?>
<?php if ($error): ?><div class="error"><?= ef_h($error) ?></div><?php endif; ?>

<?php if ($showModelPicker): ?>
    <h1 class="title">Device model</h1>
    <p class="subtitle">(This page will be redone before the final release!, only tested phones are here for now)</p>
    <form method="post" class="model-grid">
        <?php foreach ($models as $model): ?>
            <button class="button" type="submit" name="model" value="<?= ef_h($model) ?>"><?= ef_h($model) ?></button>
        <?php endforeach; ?>
    </form>
<?php else: ?>
    <div class="topbar">
        <form method="post">
            <button class="button secondary" type="submit" name="model" value="">Back</button>
        </form>
    </div>
    <div class="selected-model">Selected model: <?= ef_h($selectedModel) ?></div>
    <form method="post" class="grid">
        <input type="hidden" name="model" value="<?= ef_h($selectedModel) ?>">
        <div class="row"><label>MAC Address</label><input class="control" name="macaddr" value="<?= ef_h($values['macaddr']) ?>" placeholder="SEP001122334455" required></div>
        <div class="row"><label>Name</label><input class="control" name="name" value="<?= ef_h($values['name']) ?>"></div>
        <div class="row"><label>IPv4 Address</label><input class="control" name="ipv4" value="<?= ef_h($values['ipv4']) ?>"></div>
        <label class="check"><input type="checkbox" name="unchecked" value="1" <?= !empty($values['unchecked']) ? 'checked' : '' ?>> Do not check status of device (may slow sending of broadcasts)</label>
        <div class="row">
            <label>Audio</label>
            <select class="control" name="audio">
                <?php foreach ($audioModes as $option): ?><option <?= selected($values['audio'], $option) ?>><?= ef_h($option) ?></option><?php endforeach; ?>
            </select>
            <div class="audio-key">
                <div><strong>Multicast:</strong> Sends a single RTP stream for all phones receiving a page. Uses less server resources, less delay. Requires multicast compatible network infrastructure. High amount of packet loss on weak WLAN. Does not usually transmit over NAT/WAN & VPN tunnels. Enable IGMP on your network switch(es) for the best results.</div>
                <div><strong>Unicast:</strong> Sends RTP streams directly to the phone. Works better over WAN, VPN, and WLAN. Uses more server resources, may cause noticeable delay between speakers. Use Unicast only if Multicast cannot be used on your network.</div>
                <div><strong>Disabled:</strong> Audio will not be sent to this telephone.</div>
            </div>
        </div>
        <div class="row"><label>Visual</label><select class="control" name="visual"><?php foreach ($activeVisualModes as $option): ?><option <?= selected($values['visual'], $option) ?>><?= ef_h($option) ?></option><?php endforeach; ?></select></div>
        <div class="row"><label>Volume</label><select class="control" name="volume"><?php foreach ($volumes as $option): ?><option <?= selected($values['volume'], $option) ?>><?= ef_h($option) ?></option><?php endforeach; ?></select></div>
        <button class="button" type="submit">Add Cisco Enterprise Endpoint</button>
    </form>
<?php endif; ?>
</body>
</html>