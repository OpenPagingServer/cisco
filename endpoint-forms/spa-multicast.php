<?php
function ef_h($value) { return htmlspecialchars((string)$value, ENT_QUOTES, 'UTF-8'); }
$message = '';
$error = '';
$values = ['name' => '', 'address' => '', 'port' => ''];
try {
    $pdo->exec("CREATE TABLE IF NOT EXISTS `endpoints-output-cisco-spamulticast` (`id` INT NOT NULL AUTO_INCREMENT, `name` VARCHAR(100) NOT NULL DEFAULT '', `address` VARCHAR(45) NOT NULL DEFAULT '', `port` INT NOT NULL DEFAULT 0, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci");
    if ($_SERVER['REQUEST_METHOD'] === 'POST') {
        foreach ($values as $key => $_default) {
            $values[$key] = trim((string)($_POST[$key] ?? ''));
        }
        if ($values['name'] === '' || $values['address'] === '' || $values['port'] === '') {
            throw new RuntimeException('Name, address, and port are required.');
        }
        $port = (int)$values['port'];
        if ($port < 1 || $port > 65535) {
            throw new RuntimeException('Port must be between 1 and 65535.');
        }
        $stmt = $pdo->prepare("INSERT INTO `endpoints-output-cisco-spamulticast` (`name`, `address`, `port`) VALUES (:name, :address, :port)");
        $stmt->execute(['name' => $values['name'], 'address' => $values['address'], 'port' => $port]);
        $message = 'Cisco SPA multicast endpoint added.';
        $values = ['name' => '', 'address' => '', 'port' => ''];
    }
} catch (Throwable $exc) {
    $error = $exc->getMessage();
}
?>
<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1"><style>body{font-family:Tahoma,sans-serif;margin:0;padding:18px;color:#202124;background:#fff}.grid{display:grid;gap:12px}.row{display:grid;gap:6px}label{font-weight:500}.control{padding:10px;border:1px solid #ddd;border-radius:4px;font:inherit}.button{background:#1976D2;color:#fff;border:0;border-radius:4px;padding:10px 14px;font:inherit;cursor:pointer}.success{background:#E8F5E9;border:1px solid #A5D6A7;color:#1B5E20;padding:10px;border-radius:6px;margin-bottom:12px}.error{background:#FFEBEE;border:1px solid #EF9A9A;color:#B71C1C;padding:10px;border-radius:6px;margin-bottom:12px}@media(prefers-color-scheme:dark){body{background:#1e1e1e;color:#e0e0e0}.control{background:#171717;border-color:#333;color:#eee}.button{background:#BB86FC;color:#000}}</style></head><body>
<?php if ($message): ?><div class="success"><?= ef_h($message) ?></div><?php endif; ?><?php if ($error): ?><div class="error"><?= ef_h($error) ?></div><?php endif; ?>
<form method="post" class="grid"><div class="row"><label>Name</label><input class="control" name="name" value="<?= ef_h($values['name']) ?>" required></div><div class="row"><label>Multicast Address</label><input class="control" name="address" value="<?= ef_h($values['address']) ?>" placeholder="224.168.168.168" required></div><div class="row"><label>Port</label><input class="control" type="number" name="port" min="1" max="65535" value="<?= ef_h($values['port']) ?>" required></div><button class="button" type="submit">Add Cisco SPA Multicast Endpoint</button></form>
</body></html>
