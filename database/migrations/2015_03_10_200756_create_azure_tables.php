<?php

use Illuminate\Database\Schema\Blueprint;
use Illuminate\Database\Migrations\Migration;

/**
 * Class CreateAzureTables
 */
class CreateAzureTables extends Migration
{

    /**
     * Run the migrations.
     *
     * @return void
     */
    public function up()
    {
        // Azure Service Configuration
        Schema::create(
            'azure_config',
            function ( Blueprint $t )
            {
                $t->integer( 'service_id' )->unsigned()->primary();
                $t->foreign( 'service_id' )->references( 'id' )->on( 'services' )->onDelete( 'cascade' );
                $t->longText( 'account_name' )->nullable();
                $t->longText( 'account_key' )->nullable();
                $t->string( 'protocol' )->nullable();
            }
        );
    }

    /**
     * Reverse the migrations.
     *
     * @return void
     */
    public function down()
    {
        // Azure Service Configuration
        Schema::dropIfExists( 'azure_config' );
    }

}
