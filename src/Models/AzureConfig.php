<?php
namespace DreamFactory\Core\Azure\Models;

use DreamFactory\Core\Models\BaseServiceConfigModel;

/**
 * Class AzureConfig
 *
 * @package DreamFactory\Core\Azure\Models
 */
class AzureConfig extends BaseServiceConfigModel
{
    protected $table = 'azure_config';

    protected $encrypted = ['account_name', 'account_key'];

    protected $fillable = ['service_id', 'account_name', 'account_key', 'protocol'];
}