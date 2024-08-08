#!/bin/bash

###################################################
# MairaDB 11.4.2 installation on Rocky Linux 8
###################################################

# Extract the VERSION_ID from the release files
version_id=$(grep '^VERSION_ID=' /etc/*-release | cut -d '=' -f 2 | tr -d '"')

# Check if VERSION_ID starts with 8.
if [[ $version_id != 8.* ]]; then
  echo "VERSION_ID does not start with 8. Exiting."
  exit 1
fi

# Create group and user for MariaDB
groupadd mysql
useradd -g mysql mysql

# Set MariaDB.repo file path
REPO_FILE="/etc/yum.repos.d/MariaDB.repo"

# MariaDB.repo file content
REPO_CONTENT="[MariaDB-11.4]
name=MariaDB 11.4 repo (build 46870)
baseurl=http://mirror.mariadb.org/yum/11.4.2/rockylinux8-amd64/
gpgcheck=0
module_hotfixes=1
"

# create MariaDB.repo file
echo "$REPO_CONTENT" | sudo tee $REPO_FILE > /dev/null

# update the dnf cache
sudo dnf makecache

# check the MariaDB repo
sudo dnf repolist | grep MariaDB-11.4
if [ $? -eq 0 ]; then
    echo "MariaDB repository creation was successful."
else
    echo "MariaDB repository creation was FAILED!"
fi

# install the MariaDB server
sudo dnf -y install MariaDB

# install the MariaDB client
sudo dnf -y install mariadb

# Check if MariaDB is installed by looking for the mariadb service
if sudo systemctl list-units --type=service --all | grep -q 'mariadb.service'; then
  echo "MariaDB is installed."
else
  echo "MariaDB is not installed."
fi

# Configure the MariaDB service to start automatically at boot
sudo systemctl enable mariadb

# Start MariaDB service
sudo systemctl start mariadb

# Define the new root password
ROOT_PASSWORD="time2013"

# Execute SQL commands to configure MariaDB
sudo mysql -u root <<EOF

ALTER USER 'root'@'localhost' IDENTIFIED BY '${ROOT_PASSWORD}';
DELETE FROM mysql.user WHERE User='';
GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' IDENTIFIED BY '${ROOT_PASSWORD}' WITH GRANT OPTION;
DROP DATABASE IF EXISTS test;
DELETE FROM mysql.db WHERE Db='test' OR Db='test\\_%';
FLUSH PRIVILEGES;

grant all on nxm.* to nxm@localhost identified by 'dprtm2017!' ;
grant all on nxm.* to nxm@'127.0.0.1' identified by 'dprtm2017!' ;
GRANT FILE ON *.* TO nxm@localhost;
GRANT FILE ON *.* TO nxm@'127.0.0.1';
flush privileges;

EOF

# Update the MariaDB configuration to allow remote access
if grep -q "^bind-address" /etc/my.cnf /etc/my.cnf.d/server.cnf; then
  sed -i '/^bind-address/s/^/#/' /etc/my.cnf /etc/my.cnf.d/server.cnf
else
  echo "bind-address not found, assuming remote access is already allowed."
fi

# Restart MariaDB service to apply changes
sudo systemctl restart mariadb

echo "MariaDB configuration completed."
