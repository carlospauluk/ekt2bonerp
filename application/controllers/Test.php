<?php

class Test extends CI_Controller {

    public function teste($str) {
        echo "SAY: " . $str;
    }
    
    public function env() {
        echo "ENV: " . ENVIRONMENT;
    }

}
