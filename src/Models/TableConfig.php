<?php
namespace DreamFactory\Core\Azure\Models;

use DreamFactory\Core\Database\Components\SupportsUpsert;

class TableConfig extends AzureConfig
{
    use SupportsUpsert;
}