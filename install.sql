CREATE TABLE IF NOT EXISTS `endpoints-output-cisco` (
  `macaddr` VARCHAR(64) NOT NULL,
  `name` VARCHAR(255) DEFAULT '',
  `ipv4` VARCHAR(45) DEFAULT '',
  `status` ENUM('New', 'Unchecked', 'Offline', 'Online') NOT NULL DEFAULT 'Unchecked',
  `audio` ENUM('Multicast', 'Unicast', 'Disabled') NOT NULL DEFAULT 'Multicast',
  `model` ENUM('', '7821', '7841', '7861', '7925', '7926', '7931', '7940', '7941', '7942', '7945', '7960', '7961', '7962', '7965', '7970', '7971', '7975', '8811', '8841', '8845', '8851', '8861', '8865', '8875') NOT NULL DEFAULT '',
  `visual` ENUM('None', 'Text', 'Image') NOT NULL DEFAULT 'Image',
  `volume` ENUM('0', '10', '20', '30', '40', '50', '60', '70', '80', '90', '100', 'asis') NOT NULL DEFAULT 'asis',
  `addedby` ENUM('MANUAL', 'UCM') NOT NULL DEFAULT 'MANUAL',
  PRIMARY KEY (`macaddr`),
  KEY `ipv4_idx` (`ipv4`),
  KEY `status_idx` (`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

CREATE TABLE IF NOT EXISTS `endpoints-output-cisco-spamulticast` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(100) NOT NULL DEFAULT '',
  `address` VARCHAR(45) NOT NULL DEFAULT '',
  `port` INT NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

CREATE TABLE IF NOT EXISTS `endpoints-output-cisco-spaxmlexe` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `ipv4` VARCHAR(45) NOT NULL DEFAULT '',
  `username` VARCHAR(255) NOT NULL DEFAULT '',
  `password` VARCHAR(255) NOT NULL DEFAULT '',
  `macaddress` VARCHAR(64) NOT NULL DEFAULT '',
  `status` ENUM('New', 'Unchecked', 'Offline', 'Online') NOT NULL DEFAULT 'Unchecked',
  PRIMARY KEY (`id`),
  KEY `macaddress_idx` (`macaddress`),
  KEY `ipv4_idx` (`ipv4`),
  KEY `status_idx` (`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

CREATE TABLE IF NOT EXISTS `endpoints-modulesettings-cisco` (
  `parameter` VARCHAR(128) NOT NULL,
  `value` TEXT,
  PRIMARY KEY (`parameter`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

INSERT INTO `endpoints-modulesettings-cisco` (`parameter`, `value`) VALUES
  ('messageinfo-enabled', '1'),
  ('messageinfo-showsender', '1'),
  ('messageinfo-productname', '1'),
  ('ucmsync', '0'),
  ('ucmsync-ip', ''),
  ('ucmsync-username', ''),
  ('ucmsync-password', ''),
  ('ucmsync-interval', '300'),
  ('authrelay', 'false')
ON DUPLICATE KEY UPDATE `parameter` = `parameter`;
