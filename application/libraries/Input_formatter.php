<?php

    namespace CIBases\Libraries;

    use CIBases\Libraries\Datetime_formatter;

    class Input_formatter {

        static public function to_db($formatter, $value) {
            return Input_formatter::build_formatter($formatter)->input2db($value);
        }

        static public function to_input($formatter, $value) {
            return Input_formatter::build_formatter($formatter)->db2input($value);
        }

        static public function build_formatter($formatter) {
            switch ($formatter) {
                case "datetime2date":
                    return new Datetime_formatter('d/m/Y', 'Y-m-d H:i:s');
                case "datetime":
                    return new Datetime_formatter('d/m/Y H:i:s', 'Y-m-d H:i:s');
                default:
                    return new Input_formatter();
            }
        }

        public function input2db($value) {
            return $value;
        }

        public function db2input($value) {
            return $value;
        }

    }

