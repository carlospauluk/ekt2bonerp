<?php
defined('BASEPATH') or exit('No direct script access allowed');

class Datetime_library
{

    public function dateStrToSqlDate($dateStr, $default = null)
    {
        $dateTime = DateTime::createFromFormat('d/m/Y', $dateStr);
        if ($dateTime instanceof DateTime) {
            return $dateTime->format('Y-m-d');
        } else {
            return $default ? $default : '1900-01-01';
        }
    }

    public function datetimeStrToSqlDatetime($dateStr, $default = null)
    {
        $dateTime = DateTime::createFromFormat('d/m/Y', $dateStr);
        if ($dateTime instanceof DateTime) {
            return $dateTime->format('Y-m-d H:i:s');
        } else {
            return $default ? $default : '1900-01-01';
        }
    }
}
    