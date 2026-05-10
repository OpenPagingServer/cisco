<?php
function cisco_form_h($value) {
    return htmlspecialchars((string)$value, ENT_QUOTES, 'UTF-8');
}

$module = $module ?? 'cisco';
$forms = is_array($forms ?? null) ? $forms : (require __DIR__ . '/forms.php');
?>
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
body{font-family:Tahoma,sans-serif;margin:0;padding:18px;color:#202124;background:#fff}.title{font-size:1.25em;font-weight:500;margin:0 0 14px}.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:12px}.card{display:flex;flex-direction:column;gap:8px;min-height:112px;padding:16px;border:1px solid #ddd;border-radius:8px;text-decoration:none;color:inherit;background:#fff;box-shadow:0 2px 4px rgba(0,0,0,.08)}.card:hover,.card:focus{border-color:#1976D2;box-shadow:0 0 0 2px rgba(25,118,210,.15);outline:none}.name{font-weight:500}.desc{color:#555;line-height:1.4;font-size:.95em}@media(prefers-color-scheme:dark){body{background:#1e1e1e;color:#e0e0e0}.card{background:#171717;border-color:#333}.card:hover,.card:focus{border-color:#BB86FC;box-shadow:0 0 0 2px rgba(187,134,252,.18)}.desc{color:#bbb}}
</style>
</head>
<body>
<h2 class="title">Cisco endpoint type</h2>
<div class="grid">
    <?php foreach ($forms as $type => $form): ?>
        <a class="card" href="/admin/endpoint-form-frame.php?module=<?= urlencode($module) ?>&type=<?= urlencode((string)$type) ?>">
            <span class="name"><?= cisco_form_h($form['label'] ?? $type) ?></span>
            <?php if (!empty($form['description'])): ?>
                <span class="desc"><?= cisco_form_h($form['description']) ?></span>
            <?php endif; ?>
        </a>
    <?php endforeach; ?>
</div>
</body>
</html>
