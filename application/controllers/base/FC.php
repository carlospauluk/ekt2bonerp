<?php

/**
 * FC: Form Components
 */
class FC extends CI_Controller {
    
    public static function select($label, $id, $data, $sel) {
        $comp = "<label for=\"exampleFormControlSelect1\">" . $label . ":</label>"
                . "<select class=\"form-control\" id=\"" . $id . "\">";
        foreach ($data as $d) {
            $comp .= "<option>" . $d['label'] . "</option>";
        }
        
        $comp .= "</select>";
        
    }
    
    
    public static function checkbox() {
        
        $data = json_decode($this->input->post('data'));
        
        $label = "Modelos";
        $id = "div_checkss";
        
        echo "<legend class=\"col-form-label col-sm-2 pt-0\">" . $label . "</legend>"
        . "<div class=\"col-sm-10\" id=\"" . $id . ">";
            
                
        foreach ($data as $d) {
//            echo "                <div class=\"form-check form-check-inline\">";
//            echo "        <input class=\"form-check-input\" type=\"checkbox\" id=\"inlineCheckbox1\" value="option1">
//                    <label class="form-check-label" for="inlineCheckbox1">1</label>
//                </div>
            
        }
        
        
        
                
        echo "</div>";
    }
    
}

