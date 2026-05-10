<?php
function ef_h($value) { return htmlspecialchars((string)$value, ENT_QUOTES, 'UTF-8'); }
$message = '';
$error = '';
$values = ['ipv4' => '', 'username' => '', 'password' => '', 'macaddress' => '', 'unchecked' => ''];
try {
    $pdo->exec("CREATE TABLE IF NOT EXISTS `endpoints-output-cisco-spaxmlexe` (`id` INT NOT NULL AUTO_INCREMENT, `ipv4` VARCHAR(45) NOT NULL DEFAULT '', `username` VARCHAR(255) NOT NULL DEFAULT '', `password` VARCHAR(255) NOT NULL DEFAULT '', `macaddress` VARCHAR(64) NOT NULL DEFAULT '', `status` ENUM('New', 'Unchecked', 'Offline', 'Online') NOT NULL DEFAULT 'Unchecked', PRIMARY KEY (`id`), KEY `macaddress_idx` (`macaddress`), KEY `ipv4_idx` (`ipv4`), KEY `status_idx` (`status`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci");
    if ($_SERVER['REQUEST_METHOD'] === 'POST') {
        foreach ($values as $key => $_default) {
            $values[$key] = trim((string)($_POST[$key] ?? $_default));
        }
        $values['unchecked'] = isset($_POST['unchecked']) ? '1' : '';
        $values['macaddress'] = strtoupper(preg_replace('/[^A-Za-z0-9]/', '', $values['macaddress']));
        if ($values['ipv4'] === '' || $values['macaddress'] === '') {
            throw new RuntimeException('IPv4 address and MAC address are required.');
        }
        $status = isset($_POST['unchecked']) ? 'Unchecked' : 'New';
        $stmt = $pdo->prepare("SELECT COUNT(*) FROM `endpoints-output-cisco-spaxmlexe` WHERE `macaddress` = :macaddress");
        $stmt->execute(['macaddress' => $values['macaddress']]);
        if ((int)$stmt->fetchColumn() > 0) {
            throw new RuntimeException('That SPA EXE MAC address already exists.');
        }
        $stmt = $pdo->prepare("INSERT INTO `endpoints-output-cisco-spaxmlexe` (`ipv4`, `username`, `password`, `macaddress`, `status`) VALUES (:ipv4, :username, :password, :macaddress, :status)");
        $stmt->execute([
            'ipv4' => $values['ipv4'],
            'username' => $values['username'],
            'password' => $values['password'],
            'macaddress' => $values['macaddress'],
            'status' => $status,
        ]);
        $message = 'Cisco SPA EXE endpoint added.';
        $values = ['ipv4' => '', 'username' => '', 'password' => '', 'macaddress' => '', 'unchecked' => ''];
    }
} catch (Throwable $exc) {
    $error = $exc->getMessage();
}
?>
<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1"><style>body{font-family:Tahoma,sans-serif;margin:0;padding:18px;color:#202124;background:#fff}.grid{display:grid;gap:12px}.row{display:grid;gap:6px}label{font-weight:500}.check{display:flex;align-items:center;gap:8px;font-weight:400}.control{padding:10px;border:1px solid #ddd;border-radius:4px;font:inherit}.button{background:#1976D2;color:#fff;border:0;border-radius:4px;padding:10px 14px;font:inherit;cursor:pointer}.success{background:#E8F5E9;border:1px solid #A5D6A7;color:#1B5E20;padding:10px;border-radius:6px;margin-bottom:12px}.error{background:#FFEBEE;border:1px solid #EF9A9A;color:#B71C1C;padding:10px;border-radius:6px;margin-bottom:12px}@media(prefers-color-scheme:dark){body{background:#1e1e1e;color:#e0e0e0}.control{background:#171717;border-color:#333;color:#eee}.button{background:#BB86FC;color:#000}}</style></head><body>
<?php if ($message): ?><div class="success"><?= ef_h($message) ?></div><?php endif; ?><?php if ($error): ?><div class="error"><?= ef_h($error) ?></div><?php endif; ?>
<form method="post" class="grid"><div class="row"><label>IPv4 Address</label><input class="control" name="ipv4" value="<?= ef_h($values['ipv4']) ?>" required></div><div class="row"><label>Username</label><input class="control" name="username" value="<?= ef_h($values['username']) ?>"></div><div class="row"><label>Password</label><input class="control" type="password" name="password" value="<?= ef_h($values['password']) ?>"></div><div class="row"><label>MAC Address</label><input class="control" name="macaddress" value="<?= ef_h($values['macaddress']) ?>" required></div><label class="check"><input type="checkbox" name="unchecked" value="1" <?= !empty($values['unchecked']) ? 'checked' : '' ?>> Do not check status</label><button class="button" type="submit">Add Cisco SPA EXE Endpoint</button></form>
</body></html>