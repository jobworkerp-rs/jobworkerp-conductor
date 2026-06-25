use anyhow::Result;
use async_trait::async_trait;
use infra::infra::config_management::rdb::{
    ConfigManagementRepository, ConfigManagementRepositoryImpl, UseConfigManagementRepository,
};

#[async_trait]
pub trait ConfigManagementApp:
    UseConfigManagementRepository + Send + Sync + Sized + 'static
{
    /// Clear all configuration data
    async fn clear_all_configs(&self) -> Result<()> {
        self.config_management_repository()
            .clear_all_configs()
            .await
    }

    /// Import configuration from TOML content
    async fn import_toml_config(
        &self,
        toml_content: &str,
        overwrite_existing: bool,
    ) -> Result<(i32, i32, i32)> {
        self.config_management_repository()
            .import_toml_config(toml_content, overwrite_existing)
            .await
    }

    /// Export current configuration to TOML format
    async fn export_toml_config(&self, enabled_only: bool) -> Result<String> {
        self.config_management_repository()
            .export_toml_config(enabled_only)
            .await
    }

    /// Validate TOML configuration without importing
    async fn validate_toml_config(&self, toml_content: &str) -> Result<()> {
        // Use the validate function from the repository module
        infra::infra::config_management::rdb::validate_toml_config(toml_content)?;
        Ok(())
    }
}

pub struct ConfigManagementAppImpl {
    config_management_repository: ConfigManagementRepositoryImpl,
}

impl ConfigManagementAppImpl {
    pub fn new(config_management_repository: ConfigManagementRepositoryImpl) -> Self {
        Self {
            config_management_repository,
        }
    }
}

impl UseConfigManagementRepository for ConfigManagementAppImpl {
    fn config_management_repository(&self) -> &ConfigManagementRepositoryImpl {
        &self.config_management_repository
    }
}

impl ConfigManagementApp for ConfigManagementAppImpl {
    // Default implementations from trait are used
}
