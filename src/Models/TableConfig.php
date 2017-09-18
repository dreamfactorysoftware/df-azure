<?php
namespace DreamFactory\Core\Azure\Models;

use DreamFactory\Core\Database\Components\SupportsUpsertAndCache;

class TableConfig extends AzureConfig
{
    use SupportsUpsertAndCache;
}