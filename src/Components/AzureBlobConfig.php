<?php
namespace DreamFactory\Core\Azure\Components;

use DreamFactory\Core\Azure\Models\AzureConfig;
use DreamFactory\Core\Components\FileServiceWithContainer;
use DreamFactory\Core\Contracts\ServiceConfigHandlerInterface;
use DreamFactory\Core\Models\FilePublicPath;
use DreamFactory\Library\Utility\ArrayUtils;

class AzureBlobConfig implements ServiceConfigHandlerInterface
{
    use FileServiceWithContainer;

    /**
     * @param int $id
     *
     * @return array
     */
    public static function getConfig($id)
    {
        $azureConfig = AzureConfig::find($id);
        $pathConfig = FilePublicPath::find($id);

        $config = [];

        if (!empty($azureConfig)) {
            $config = $azureConfig->toArray();
        }

        if (!empty($pathConfig)) {
            $config = array_merge($config, $pathConfig->toArray());
        }

        return $config;
    }

    /**
     * {@inheritdoc}
     */
    public static function validateConfig($config)
    {
        return true;
    }

    /**
     * {@inheritdoc}
     */
    public static function setConfig($id, $config)
    {
        $azureConfig = AzureConfig::find($id);
        $pathConfig = FilePublicPath::find($id);
        $configPath = [
            'public_path' => ArrayUtils::get($config, 'public_path'),
            'container'   => ArrayUtils::get($config, 'container')
        ];
        $configAzure = [
            'service_id'   => ArrayUtils::get($config, 'service_id'),
            'account_name' => ArrayUtils::get($config, 'account_name'),
            'account_key'  => ArrayUtils::get($config, 'account_key'),
            'protocol'     => ArrayUtils::get($config, 'protocol')
        ];

        ArrayUtils::removeNull($configAzure);
        ArrayUtils::removeNull($configPath);

        if (!empty($azureConfig)) {
            $azureConfig->update($configAzure);
        } else {
            //Making sure service_id is the first item in the config.
            //This way service_id will be set first and is available
            //for use right away. This helps setting an auto-generated
            //field that may depend on parent data. See OAuthConfig->setAttribute.
            $configAzure = array_reverse($configAzure, true);
            $configAzure['service_id'] = $id;
            $configAzure = array_reverse($configAzure, true);
            AzureConfig::create($configAzure);
        }

        if (!empty($pathConfig)) {
            $pathConfig->update($configPath);
        } else {
            //Making sure service_id is the first item in the config.
            //This way service_id will be set first and is available
            //for use right away. This helps setting an auto-generated
            //field that may depend on parent data. See OAuthConfig->setAttribute.
            $configPath = array_reverse($configPath, true);
            $configPath['service_id'] = $id;
            $configPath = array_reverse($configPath, true);
            FilePublicPath::create($configPath);
        }
    }

    /**
     * {@inheritdoc}
     */
    public static function getConfigSchema()
    {
        $azureConfig = new AzureConfig();
        $pathConfig = new FilePublicPath();
        $out = null;

        $azureSchema = $azureConfig->getConfigSchema();
        $pathSchema = $pathConfig->getConfigSchema();

        static::updatePathSchema($pathSchema);

        if (!empty($azureSchema)) {
            $out = $azureSchema;
        }
        if (!empty($pathSchema)) {
            $out = ($out) ? array_merge($out, $pathSchema) : $pathSchema;
        }

        return $out;
    }

    /**
     * {@inheritdoc}
     */
    public static function removeConfig($id)
    {
        // deleting is not necessary here due to cascading on_delete relationship in database
    }

    /**
     * {@inheritdoc}
     */
    public static function getAvailableConfigs()
    {
        return null;
    }
}