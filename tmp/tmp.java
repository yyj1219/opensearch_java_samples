#!/bin/bash

###################################################
# Install OpenSearch 2.13.0 using .tar.gz on Rocky Linux 8
###################################################

# Extract the VERSION_ID from the OS release files
version_id=$(grep '^VERSION_ID=' /etc/*-release | cut -d '=' -f 2 | tr -d '"')

# Verify that the VERSION_ID starts with 8
if [[ $version_id != 8.* ]]; then
  echo "VERSION_ID does not start with 8. Exiting."
  exit 1
fi

# Get the current user
CURRENT_USER=$(whoami)

# Define the required username
TARGET_USER="tuna"

# Define the target directory
OPENSEARCH_HOME="/engn001/opensearch-2.13.0"

# Check if the current user matches the required username
if [ "$CURRENT_USER" != "$TARGET_USER" ]; then
  echo "This script must be run as user '$TARGET_USER'. Exiting."
  exit 1
fi

# Check if the current user has sudo privileges
if ! sudo -l >/dev/null 2>&1; then
  echo "This script requires sudo privileges. Exiting."
  exit 1
fi

# Swapoff
sudo swapoff -a

# Check if the setting already exists in sysctl.conf
if ! grep -q "vm.max_map_count=262144" /etc/sysctl.conf; then
  # Add the setting to sysctl.conf
  echo "vm.max_map_count=262144" | sudo tee -a /etc/sysctl.conf
fi

# Reload the kernel parameters using sysctl
sudo sysctl -p

echo "vm.max_map_count has been set to 262144 and made permanent."

# Add limits
echo "$TARGET_USER soft nproc 65536" | sudo tee -a /etc/security/limits.conf
echo "$TARGET_USER hard nproc 65536" | sudo tee -a /etc/security/limits.conf
echo "$TARGET_USER soft nofile 65536" | sudo tee -a /etc/security/limits.conf
echo "$TARGET_USER hard nofile 65536" | sudo tee -a /etc/security/limits.conf
echo "$TARGET_USER soft memlock unlimited" | sudo tee -a /etc/security/limits.conf
echo "$TARGET_USER hard memlock unlimited" | sudo tee -a /etc/security/limits.conf

# Create the directory with sudo and set the owner to the current user
sudo mkdir -p $OPENSEARCH_HOME
sudo chown $TARGET_USER:$TARGET_USER $OPENSEARCH_HOME

echo "Extracting OpenSearch to $OPENSEARCH_HOME."

# Get the directory where the script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Extract the opensearch-2.13.0.tar.gz file into the target directory
tar -xzf $SCRIPT_DIR/opensearch-2.13.0-linux-x64.tar.gz -C $OPENSEARCH_HOME --strip-components=1

echo "OpenSearch has been extracted to $OPENSEARCH_HOME and ownership set to $CURRENT_USER."

# Define the path to the opensearch.yml file
CONFIG_FILE="/engn001/opensearch-2.13.0/config/opensearch.yml"

# Check for the OPENSEARCH_DATA_DIR environment variable
if [ -n "$OPENSEARCH_DATA_DIR" ]; then
  DATA_DIR="$OPENSEARCH_DATA_DIR"
elif [ -d "/data001/opensearch" ]; then
  DATA_DIR="/data001/opensearch"
else
  DATA_DIR=""
fi

# If DATA_DIR is set, ensure tuna user and group can write to it
if [ -n "$DATA_DIR" ]; then
  sudo chown -R $TARGET_USER:$TARGET_USER "$DATA_DIR"
  sudo chmod -R 775 "$DATA_DIR"
fi

# Check for the OPENSEARCH_LOGS_DIR environment variable
if [ -n "$OPENSEARCH_LOGS_DIR" ]; then
  LOGS_DIR="$OPENSEARCH_LOGS_DIR"
elif [ -d "/logs001/opensearch" ]; then
  LOGS_DIR="/logs001/opensearch"
else
  LOGS_DIR=""
fi

# If LOGS_DIR is set, ensure tuna user and group can write to it
if [ -n "$LOGS_DIR" ]; then
  sudo chown -R $TARGET_USER:$TARGET_USER "$LOGS_DIR"
  sudo chmod -R 775 "$LOGS_DIR"
fi

# Use sed to modify the specified lines in the configuration file
sudo sed -i 's/^#cluster\.name:.*/cluster.name: tuna-ee/' "$CONFIG_FILE"
sudo sed -i 's/^#node\.name:.*/node.name: single-node/' "$CONFIG_FILE"

# Conditionally set path.data if DATA_DIR is not empty
if [ -n "$DATA_DIR" ]; then
  sudo sed -i "s|^#path\.data:.*|path.data: $DATA_DIR|" "$CONFIG_FILE"
else
  echo "No valid data directory found. path.data will not be set."
fi

# Conditionally set path.logs if LOGS_DIR is not empty
if [ -n "$LOGS_DIR" ]; then
  sudo sed -i "s|^#path\.logs:.*|path.logs: $LOGS_DIR|" "$CONFIG_FILE"
else
  echo "No valid logs directory found. path.logs will not be set."
fi

sudo sed -i 's/^#network\.host:.*/network.host: 0.0.0.0/' "$CONFIG_FILE"

echo ""                                >> $CONFIG_FILE
echo "# Configure for TunA EE"         >> $CONFIG_FILE
echo "discovery.type: single-node"     >> $CONFIG_FILE
echo "plugin.security.disabled: true"  >> $CONFIG_FILE

# Append security policy configuration
POLICY_FILE="$OPENSEARCH_HOME/config/opensearch-performance-analyzer/opensearch_security.policy"
echo "grant {"                                                   >> $POLICY_FILE
echo "  java.lang.RuntimePermission \"accessUserInformation\";"  >> $POLICY_FILE
echo "};"                                                        >> $POLICY_FILE

echo "OpenSearch Configuration has been added to opensearch.yml and opensearch_security.policy."

# Start the OpenSearch
