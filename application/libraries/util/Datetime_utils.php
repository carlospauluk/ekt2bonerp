<?php

defined('BASEPATH') OR exit('No direct script access allowed');

class Datetime_utils {

    public static function mesano2sqldate($mesano) {
        return DateTime::createFromFormat('Ymd', $mesano . "01")->format('Y-m-d');
    }

    public static function sqldate2mesano($date) {
        $date = DateTime::createFromFormat('Y-m-d', $date);
        $f = $date->format('Ym');
        return $f;
    }
    
    public static function ultimo_dia($date) {
        return date("Y-m-t", strtotime($date));
    }

    public static function inc($date, $oque, $quanto) {

        $date = new DateTime($date);
        if ($quanto > 0) {
            $quanto = "+" . $quanto;
        }
        $date->modify($quanto . ' ' . $oque);
        return $date->format('Y-m-d');
    }

    public static function inc_mesano($mesano, $oque, $quanto) {
        $dt = Datetime_utils::mesano2sqldate($mesano);
        $r = Datetime_utils::inc($dt, $oque, $quanto);
        $mesano = Datetime_utils::sqldate2mesano($r);
        return $mesano;
    }
    
    public static function mesano_list($mesano_ini, $mesano_fim) {
        $dt_ini = Datetime_utils::mesano2sqldate($mesano_ini);
        $dt_fim = Datetime_utils::mesano2sqldate($mesano_fim);
        
        $i=0;
        $aux = $dt_ini;
        while ($aux < $dt_fim) {
            $aux = Datetime_utils::inc($dt_ini, 'month', $i);
            $r[] = Datetime_utils::sqldate2mesano($aux);
            $i++;
        }
        
        return $r;
    }

}
