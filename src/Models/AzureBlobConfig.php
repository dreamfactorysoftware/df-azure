<?php
/**
 * This file is part of the DreamFactory Rave(tm)
 *
 * DreamFactory Rave(tm) <http://github.com/dreamfactorysoftware/rave>
 * Copyright 2012-2014 DreamFactory Software, Inc. <support@dreamfactory.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace DreamFactory\Core\Azure\Models;

use DreamFactory\Core\Models\FilePublicPath;
use DreamFactory\Library\Utility\ArrayUtils;
use DreamFactory\Core\SqlDbCore\ColumnSchema;
use DreamFactory\Core\Contracts\ServiceConfigHandlerInterface;

class AzureBlobConfig implements ServiceConfigHandlerInterface
{
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
            'public_path' => ArrayUtils::get($config, 'public_path')
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

    /**
     * {@inheritdoc}
     */
    public static function getConfigSchema()
    {
        $azureConfig = new AzureConfig();
        $pathConfig = new FilePublicPath();
        $out = [];

        $azureSchema = $azureConfig->getTableSchema();
        if ($azureSchema) {
            foreach ($azureSchema->columns as $name => $column) {
                if ('service_id' === $name) {
                    continue;
                }

                /** @var ColumnSchema $column */
                $out[$name] = $column->toArray();
            }
            //return $out;
        }

        $pathSchema = $pathConfig->getTableSchema();
        if ($pathSchema) {
            foreach ($pathSchema->columns as $name => $column) {
                if ('service_id' === $name) {
                    continue;
                }

                /** @var ColumnSchema $column */
                $out[$name] = $column->toArray();
            }
        }

        if (!empty($out)) {
            return $out;
        }

        return null;
    }
}