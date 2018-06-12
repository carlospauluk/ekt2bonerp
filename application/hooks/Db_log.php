<?php

class Db_log {

    function __construct() {
        
    }

    function log_queries() {
        
        $CI = & get_instance();
        if (!isset($CI->db)) return;

        $filepath = APPPATH . 'logs/Query-log-' . date('Y-m-d') . '.php';
        $handle = fopen($filepath, "a+");

        
        $times = $CI->db->query_times;
        foreach ($CI->db->queries as $key => $query) {
            $sql = $query . " \n Execution Time:" . $times[$key];

            fwrite($handle, $sql . "\n\n");
        }

        fclose($handle);
    }

}
