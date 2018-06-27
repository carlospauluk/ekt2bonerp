<?php
namespace CIBases\Libraries;

class Datetime_formatter
{

    private $input_format;

    private $db_format;

    public function __construct($input_format, $db_format)
    {
        $this->input_format = $input_format;
        $this->db_format = $db_format;
    }

    public function input2db($value)
    {
        $orig = \DateTime::createFromFormat($this->input_format, $value);
        $dest = null;
        if ($orig) {
            $dest = $orig->format($this->db_format);
        }
        return $dest;
    }

    public function db2input($value)
    {
        $orig = \DateTime::createFromFormat($this->db_format, $value);
        $dest = null;
        if ($orig) {
            $dest = $orig->format($this->input_format);
        }
        return $dest;
    }
}
