<?php

class Db_util {

    public static function insert($db, $table, $data) {
        $ret = $db->insert($table, $data);
        if ($ret) {
            $query = $db->query('SELECT LAST_INSERT_ID() as last_id');
            $row = $query->row_array();
            $last_id = $row['last_id'];
            return $last_id;
        } else {
            throw new Exception('Erro ao realizar o INSERT.');
        }
    }

}
