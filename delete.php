<?php
function cisco_endpoint_delete_h($value) {
    return htmlspecialchars((string)$value, ENT_QUOTES, 'UTF-8');
}

function cisco_endpoint_delete_normalize($value) {
    return strtoupper(preg_replace('/[^A-Za-z0-9]/', '', (string)$value));
}

function cisco_endpoint_delete_spa_exe_row($pdo, $endpointId) {
    $token = cisco_endpoint_delete_normalize(substr($endpointId, strlen('spa-exe-')));
    $stmt = $pdo->query("SELECT `id`, `ipv4`, `macaddress` FROM `endpoints-output-cisco-spaxmlexe`");
    foreach ($stmt->fetchAll(PDO::FETCH_ASSOC) as $row) {
        if (cisco_endpoint_delete_normalize($row['macaddress'] ?? '') === $token) {
            return $row;
        }
    }
    return null;
}

$message = '';
$error = '';
$row = null;
$kind = 'enterprise';
$label = $endpointId;

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
        $stmt = $pdo->prepare("SELECT `id`, `name`, `address`, `port` FROM `endpoints-output-cisco-spamulticast` WHERE `id` = :id");
        $stmt->execute(['id' => $id]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        if ($row) {
            $label = ($row['name'] ?: 'Cisco SPA multicast') . ' (' . $row['address'] . ':' . $row['port'] . ')';
        }
        if ($row && $_SERVER['REQUEST_METHOD'] === 'POST') {
            $stmt = $pdo->prepare("DELETE FROM `endpoints-output-cisco-spamulticast` WHERE `id` = :id");
            $stmt->execute(['id' => $id]);
            $message = 'Cisco SPA multicast endpoint deleted.';
            $row = null;
        }
    } elseif (strpos($endpointId, 'spa-exe-') === 0) {
        $kind = 'spa-exe';
        $row = cisco_endpoint_delete_spa_exe_row($pdo, $endpointId);
        if ($row) {
            $label = ($row['macaddress'] ?: 'Cisco SPA EXE') . ' (' . $row['ipv4'] . ')';
        }
        if ($row && $_SERVER['REQUEST_METHOD'] === 'POST') {
            $stmt = $pdo->prepare("DELETE FROM `endpoints-output-cisco-spaxmlexe` WHERE `id` = :id");
            $stmt->execute(['id' => $row['id']]);
            $message = 'Cisco SPA EXE endpoint deleted.';
            $row = null;
        }
    } else {
        $stmt = $pdo->prepare("SELECT `macaddr`, `name`, `ipv4` FROM `endpoints-output-cisco` WHERE `macaddr` = :macaddr");
        $stmt->execute(['macaddr' => $endpointId]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        if ($row) {
            $label = ($row['name'] ?: $row['macaddr']) . ($row['ipv4'] ? ' (' . $row['ipv4'] . ')' : '');
        }
        if ($row && $_SERVER['REQUEST_METHOD'] === 'POST') {
            $stmt = $pdo->prepare("DELETE FROM `endpoints-output-cisco` WHERE `macaddr` = :macaddr");
            $stmt->execute(['macaddr' => $row['macaddr']]);
            $message = 'Cisco enterprise endpoint deleted.';
            $row = null;
        }
    }

    if (!$row && $message === '') {
        throw new RuntimeException('Endpoint not found.');
    }
} catch (Throwable $exc) {
    $error = $exc->getMessage();
}
?>
<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1"><style>body{font-family:Tahoma,sans-serif;margin:0;padding:18px;color:#202124;background:#fff}.button{background:#C62828;color:#fff;border:0;border-radius:4px;padding:10px 14px;font:inherit;cursor:pointer}.success{background:#E8F5E9;border:1px solid #A5D6A7;color:#1B5E20;padding:10px;border-radius:6px;margin-bottom:12px}.error{background:#FFEBEE;border:1px solid #EF9A9A;color:#B71C1C;padding:10px;border-radius:6px;margin-bottom:12px}.warn{background:#FFF8E1;border:1px solid #FFE082;color:#5D4037;padding:12px;border-radius:6px;margin-bottom:12px}@media(prefers-color-scheme:dark){body{background:#1e1e1e;color:#e0e0e0}.warn{background:#352b10;border-color:#66511a;color:#ffe2a8}}</style></head><body>
<?php if ($message): ?><div class="success"><?= cisco_endpoint_delete_h($message) ?></div><?php endif; ?>
<?php if ($error): ?><div class="error"><?= cisco_endpoint_delete_h($error) ?></div><?php endif; ?>
<?php if ($row): ?><div class="warn">Delete <?= cisco_endpoint_delete_h($label) ?>?</div><form method="post"><button class="button" type="submit">Delete Endpoint</button></form><?php endif; ?>
</body></html>
