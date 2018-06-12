<?php

class Globals extends CI_Controller {

    public function showenv() {
        echo ENVIRONMENT;
    }
    
    public function apppath() {
        echo APPPATH;
    }

}
