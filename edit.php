<?php
function cisco_endpoint_h($value) {
    return htmlspecialchars((string)$value, ENT_QUOTES, 'UTF-8');
}

function cisco_endpoint_selected($actual, $expected) {
    return (string)$actual === (string)$expected ? 'selected' : '';
}

function cisco_endpoint_normalize_device_id($value) {
    return strtoupper(preg_replace('/[^A-Za-z0-9]/', '', (string)$value));
}

function cisco_endpoint_spa_exe_row($pdo, $endpointId) {
    $token = cisco_endpoint_normalize_device_id(substr($endpointId, strlen('spa-exe-')));
    $stmt = $pdo->query("SELECT `id`, `ipv4`, `username`, `password`, `macaddress`, `status` FROM `endpoints-output-cisco-spaxmlexe`");
    foreach ($stmt->fetchAll(PDO::FETCH_ASSOC) as $row) {
        if (cisco_endpoint_normalize_device_id($row['macaddress'] ?? '') === $token) {
            return $row;
        }
    }
    return null;
}

$message = '';
$error = '';
$kind = 'enterprise';
$row = null;
$models = ['7811', '7821', '7841', '7861', '7925', '7926', '7931', '7940', '7941', '7942', '7945', '7960', '7961', '7962', '7965', '7970', '7971', '7975', '8811', '8841', '8845', '8851', '8861', '8865', '8875'];
$audioModes = ['Multicast', 'Unicast', 'Disabled'];
$visualModes = ['None', 'Text', 'Image'];
$textOnlyModels = ['7811', '7821', '7841', '7861'];
$volumes = ['0', '10', '20', '30', '40', '50', '60', '70', '80', '90', '100', 'asis'];

try {
    $pdo->exec("CREATE TABLE IF NOT EXISTS `endpoints-output-cisco` (
        `macaddr` VARCHAR(64) NOT NULL,
        `name` VARCHAR(255) DEFAULT '',
        `ipv4` VARCHAR(45) DEFAULT '',
        `status` ENUM('New', 'Unchecked', 'Offline', 'Online') NOT NULL DEFAULT 'Unchecked',
        `audio` ENUM('Multicast', 'Unicast', 'Disabled') NOT NULL DEFAULT 'Multicast',
        `model` ENUM('', '7811', '7821', '7841', '7861', '7925', '7926', '7931', '7940', '7941', '7942', '7945', '7960', '7961', '7962', '7965', '7970', '7971', '7975', '8811', '8841', '8845', '8851', '8861', '8865', '8875') NOT NULL DEFAULT '',
        `visual` ENUM('None', 'Text', 'Image') NOT NULL DEFAULT 'Image',
        `volume` ENUM('0', '10', '20', '30', '40', '50', '60', '70', '80', '90', '100', 'asis') NOT NULL DEFAULT 'asis',
        `addedby` ENUM('MANUAL', 'UCM') NOT NULL DEFAULT 'MANUAL',
        PRIMARY KEY (`macaddr`),
        KEY `ipv4_idx` (`ipv4`),
        KEY `status_idx` (`status`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci");
    $pdo->exec("CREATE TABLE IF NOT EXISTS `endpoints-output-cisco-spamulticast` (`id` INT NOT NULL AUTO_INCREMENT, `name` VARCHAR(100) NOT NULL DEFAULT '', `address` VARCHAR(45) NOT NULL DEFAULT '', `port` INT NOT NULL DEFAULT 0, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci");
    $pdo->exec("CREATE TABLE IF NOT EXISTS `endpoints-output-cisco-spaxmlexe` (`id` INT NOT NULL AUTO_INCREMENT, `ipv4` VARCHAR(45) NOT NULL DEFAULT '', `username` VARCHAR(255) NOT NULL DEFAULT '', `password` VARCHAR(255) NOT NULL DEFAULT '', `macaddress` VARCHAR(64) NOT NULL DEFAULT '', `status` ENUM('New', 'Unchecked', 'Offline', 'Online') NOT NULL DEFAULT 'Unchecked', PRIMARY KEY (`id`), KEY `macaddress_idx` (`macaddress`), KEY `ipv4_idx` (`ipv4`), KEY `status_idx` (`status`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci");

    if (strpos($endpointId, 'spa-multicast-') === 0) {
        $kind = 'spa-multicast';
        $id = (int)substr($endpointId, strlen('spa-multicast-'));
        if ($id < 1) {
            throw new RuntimeException('Invalid Cisco SPA multicast endpoint.');
        }
        if ($_SERVER['REQUEST_METHOD'] === 'POST') {
            $values = [
                'name' => trim((string)($_POST['name'] ?? '')),
                'address' => trim((string)($_POST['address'] ?? '')),
                'port' => trim((string)($_POST['port'] ?? '')),
            ];
            if ($values['name'] === '' || $values['address'] === '' || $values['port'] === '') {
                throw new RuntimeException('Name, address, and port are required.');
            }
            $port = (int)$values['port'];
            if ($port < 1 || $port > 65535) {
                throw new RuntimeException('Port must be between 1 and 65535.');
            }
            $stmt = $pdo->prepare("UPDATE `endpoints-output-cisco-spamulticast` SET `name` = :name, `address` = :address, `port` = :port WHERE `id` = :id");
            $stmt->execute(['name' => $values['name'], 'address' => $values['address'], 'port' => $port, 'id' => $id]);
            $message = 'Cisco SPA multicast endpoint updated.';
        }
        $stmt = $pdo->prepare("SELECT `id`, `name`, `address`, `port` FROM `endpoints-output-cisco-spamulticast` WHERE `id` = :id");
        $stmt->execute(['id' => $id]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
    } elseif (strpos($endpointId, 'spa-exe-') === 0) {
        $kind = 'spa-exe';
        $lookupId = (int)($_POST['_lookup_id'] ?? 0);
        if ($lookupId > 0) {
            $stmt = $pdo->prepare("SELECT `id`, `ipv4`, `username`, `password`, `macaddress`, `status` FROM `endpoints-output-cisco-spaxmlexe` WHERE `id` = :id");
            $stmt->execute(['id' => $lookupId]);
            $row = $stmt->fetch(PDO::FETCH_ASSOC);
        } else {
            $row = cisco_endpoint_spa_exe_row($pdo, $endpointId);
        }
        if ($row && $_SERVER['REQUEST_METHOD'] === 'POST') {
            $values = [
                'ipv4' => trim((string)($_POST['ipv4'] ?? '')),
                'username' => trim((string)($_POST['username'] ?? '')),
                'password' => trim((string)($_POST['password'] ?? '')),
                'macaddress' => cisco_endpoint_normalize_device_id($_POST['macaddress'] ?? ''),
            ];
            if ($values['ipv4'] === '' || $values['macaddress'] === '') {
                throw new RuntimeException('IPv4 address and MAC address are required.');
            }
            $stmt = $pdo->prepare("SELECT COUNT(*) FROM `endpoints-output-cisco-spaxmlexe` WHERE `macaddress` = :macaddress AND `id` <> :id");
            $stmt->execute(['macaddress' => $values['macaddress'], 'id' => $row['id']]);
            if ((int)$stmt->fetchColumn() > 0) {
                throw new RuntimeException('That SPA EXE MAC address already exists.');
            }
            $stmt = $pdo->prepare("UPDATE `endpoints-output-cisco-spaxmlexe` SET `ipv4` = :ipv4, `username` = :username, `password` = :password, `macaddress` = :macaddress WHERE `id` = :id");
            $stmt->execute(['ipv4' => $values['ipv4'], 'username' => $values['username'], 'password' => $values['password'], 'macaddress' => $values['macaddress'], 'id' => $row['id']]);
            $message = 'Cisco SPA EXE endpoint updated.';
            $stmt = $pdo->prepare("SELECT `id`, `ipv4`, `username`, `password`, `macaddress`, `status` FROM `endpoints-output-cisco-spaxmlexe` WHERE `id` = :id");
            $stmt->execute(['id' => $row['id']]);
            $row = $stmt->fetch(PDO::FETCH_ASSOC);
        }
    } else {
        $kind = 'enterprise';
        $lookupMacaddr = trim((string)($_POST['_lookup_macaddr'] ?? $endpointId));
        $stmt = $pdo->prepare("SELECT `macaddr`, `name`, `ipv4`, `status`, `audio`, `model`, `visual`, `volume`, `addedby` FROM `endpoints-output-cisco` WHERE `macaddr` = :macaddr");
        $stmt->execute(['macaddr' => $lookupMacaddr]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        if ($row && $_SERVER['REQUEST_METHOD'] === 'POST') {
            $values = [
                'macaddr' => cisco_endpoint_normalize_device_id($_POST['macaddr'] ?? ''),
                'name' => trim((string)($_POST['name'] ?? '')),
                'ipv4' => trim((string)($_POST['ipv4'] ?? '')),
                'audio' => trim((string)($_POST['audio'] ?? 'Multicast')),
                'model' => trim((string)($_POST['model'] ?? '')),
                'visual' => trim((string)($_POST['visual'] ?? 'Image')),
                'volume' => trim((string)($_POST['volume'] ?? 'asis')),
            ];
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
            if (!in_array($values['audio'], $audioModes, true)) {
                $values['audio'] = 'Multicast';
            }
            if (!in_array($values['visual'], $activeVisualModes, true)) {
                $values['visual'] = in_array($values['model'], $textOnlyModels, true) ? 'Text' : 'Image';
            }
            if (!in_array($values['volume'], $volumes, true)) {
                $values['volume'] = 'asis';
            }
            $stmt = $pdo->prepare("SELECT COUNT(*) FROM `endpoints-output-cisco` WHERE `macaddr` = :macaddr AND `macaddr` <> :oldmac");
            $stmt->execute(['macaddr' => $values['macaddr'], 'oldmac' => $row['macaddr']]);
            if ((int)$stmt->fetchColumn() > 0) {
                throw new RuntimeException('That Cisco SEP endpoint already exists.');
            }
            $stmt = $pdo->prepare("UPDATE `endpoints-output-cisco` SET `macaddr` = :macaddr, `name` = :name, `ipv4` = :ipv4, `audio` = :audio, `model` = :model, `visual` = :visual, `volume` = :volume WHERE `macaddr` = :oldmac");
            $stmt->execute(['macaddr' => $values['macaddr'], 'name' => $values['name'], 'ipv4' => $values['ipv4'], 'audio' => $values['audio'], 'model' => $values['model'], 'visual' => $values['visual'], 'volume' => $values['volume'], 'oldmac' => $row['macaddr']]);
            $message = 'Cisco enterprise endpoint updated.';
            $stmt = $pdo->prepare("SELECT `macaddr`, `name`, `ipv4`, `status`, `audio`, `model`, `visual`, `volume`, `addedby` FROM `endpoints-output-cisco` WHERE `macaddr` = :macaddr");
            $stmt->execute(['macaddr' => $values['macaddr']]);
            $row = $stmt->fetch(PDO::FETCH_ASSOC);
        }
    }

    if (!$row) {
        throw new RuntimeException('Endpoint not found.');
    }
} catch (Throwable $exc) {
    $error = $exc->getMessage();
}

$activeVisualModes = $row && $kind === 'enterprise' && in_array((string)$row['model'], $textOnlyModels, true) ? ['None', 'Text'] : $visualModes;
?>
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<style>
body{font-family:Tahoma,sans-serif;margin:0;padding:18px;color:#202124;background:#fff}.grid{display:grid;gap:12px}.row{display:grid;gap:6px}label{font-weight:500}.control{padding:10px;border:1px solid #ddd;border-radius:4px;font:inherit}.button{background:#1976D2;color:#fff;border:0;border-radius:4px;padding:10px 14px;font:inherit;cursor:pointer;text-decoration:none;display:inline-flex;align-items:center;justify-content:center}.success{background:#E8F5E9;border:1px solid #A5D6A7;color:#1B5E20;padding:10px;border-radius:6px;margin-bottom:12px}.error{background:#FFEBEE;border:1px solid #EF9A9A;color:#B71C1C;padding:10px;border-radius:6px;margin-bottom:12px}.meta{color:#5f6368;margin:0 0 14px}@media(prefers-color-scheme:dark){body{background:#1e1e1e;color:#e0e0e0}.control{background:#171717;border-color:#333;color:#eee}.button{background:#BB86FC;color:#000}.meta{color:#aaa}}
</style>
</head>
<body>
<?php if ($message): ?><div class="success"><?= cisco_endpoint_h($message) ?></div><?php endif; ?>
<?php if ($error): ?><div class="error"><?= cisco_endpoint_h($error) ?></div><?php endif; ?>
<?php if ($row && $kind === 'enterprise'): ?>
    <p class="meta">Current status: <?= cisco_endpoint_h($row['status'] ?? '') ?></p>
    <form method="post" class="grid">
        <input type="hidden" name="_lookup_macaddr" value="<?= cisco_endpoint_h($row['macaddr'] ?? '') ?>">
        <div class="row"><label>MAC Address</label><input class="control" name="macaddr" value="<?= cisco_endpoint_h($row['macaddr'] ?? '') ?>" required></div>
        <div class="row"><label>Name</label><input class="control" name="name" value="<?= cisco_endpoint_h($row['name'] ?? '') ?>"></div>
        <div class="row"><label>IPv4 Address</label><input class="control" name="ipv4" value="<?= cisco_endpoint_h($row['ipv4'] ?? '') ?>"></div>
        <div class="row"><label>Model</label><select class="control" name="model"><?php foreach ($models as $option): ?><option <?= cisco_endpoint_selected($row['model'] ?? '', $option) ?>><?= cisco_endpoint_h($option) ?></option><?php endforeach; ?></select></div>
        <div class="row"><label>Audio</label><select class="control" name="audio"><?php foreach ($audioModes as $option): ?><option <?= cisco_endpoint_selected($row['audio'] ?? '', $option) ?>><?= cisco_endpoint_h($option) ?></option><?php endforeach; ?></select></div>
        <div class="row"><label>Visual</label><select class="control" name="visual"><?php foreach ($activeVisualModes as $option): ?><option <?= cisco_endpoint_selected($row['visual'] ?? '', $option) ?>><?= cisco_endpoint_h($option) ?></option><?php endforeach; ?></select></div>
        <div class="row"><label>Volume</label><select class="control" name="volume"><?php foreach ($volumes as $option): ?><option <?= cisco_endpoint_selected($row['volume'] ?? '', $option) ?>><?= cisco_endpoint_h($option) ?></option><?php endforeach; ?></select></div>
        <button class="button" type="submit">Save Cisco Enterprise Endpoint</button>
    </form>
<?php elseif ($row && $kind === 'spa-multicast'): ?>
    <form method="post" class="grid">
        <div class="row"><label>Name</label><input class="control" name="name" value="<?= cisco_endpoint_h($row['name'] ?? '') ?>" required></div>
        <div class="row"><label>Multicast Address</label><input class="control" name="address" value="<?= cisco_endpoint_h($row['address'] ?? '') ?>" required></div>
        <div class="row"><label>Port</label><input class="control" type="number" name="port" min="1" max="65535" value="<?= cisco_endpoint_h($row['port'] ?? '') ?>" required></div>
        <button class="button" type="submit">Save Cisco SPA Multicast Endpoint</button>
    </form>
<?php elseif ($row && $kind === 'spa-exe'): ?>
    <p class="meta">Current status: <?= cisco_endpoint_h($row['status'] ?? '') ?></p>
    <form method="post" class="grid">
        <input type="hidden" name="_lookup_id" value="<?= cisco_endpoint_h($row['id'] ?? '') ?>">
        <div class="row"><label>IPv4 Address</label><input class="control" name="ipv4" value="<?= cisco_endpoint_h($row['ipv4'] ?? '') ?>" required></div>
        <div class="row"><label>Username</label><input class="control" name="username" value="<?= cisco_endpoint_h($row['username'] ?? '') ?>"></div>
        <div class="row"><label>Password</label><input class="control" type="password" name="password" value="<?= cisco_endpoint_h($row['password'] ?? '') ?>"></div>
        <div class="row"><label>MAC Address</label><input class="control" name="macaddress" value="<?= cisco_endpoint_h($row['macaddress'] ?? '') ?>" required></div>
        <button class="button" type="submit">Save Cisco SPA EXE Endpoint</button>
    </form>
<?php endif; ?>
</body>
</html>
