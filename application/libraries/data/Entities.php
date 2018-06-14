<?php

defined('BASEPATH') OR exit('No direct script access allowed');

class Entities {

    public static function default_values() {
        $entity['estabelecimento_id'] = 1;
        $entity['user_inserted_id'] = 1;
        $entity['user_updated_id'] = 1;
        $entity['version'] = 0;
        $now = new DateTime("now", new DateTimeZone('- 0300'));
        $entity['updated'] = $now->format('Y-m-d H:i:s');
        $entity['inserted'] = $now->format('Y-m-d H:i:s');
        return $entity;
    }
    
    
    public static function upd($entity) {
        $now = new DateTime("now", new DateTimeZone('- 0300'));
        $entity['updated'] = $now->format('Y-m-d H:i:s');
        return $entity;
    }

}
