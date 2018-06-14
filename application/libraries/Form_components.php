<?php

namespace CIBases\Libraries;

/**
 * Biblioteca para auxiliar na criação de componentes de formulários e reduzir duplicações de códigos.
 */
class Form_components {

	public function __construct() {
		// Assign the CodeIgniter super-object
		$CI = &get_instance ();
		$CI->load->library ( 'session' );
		$this->session = $CI->session;
	}

	public function handle_error($field) {
		if (form_error ( $field )) {
			$error_msg = form_error ( $field, '<span class="help-block" style="font-size: smaller">* ', '</span>' );
			$error_class = "has-error";
			
			return array (
					'class' => $error_class,
					'span' => $error_msg 
			);
		} else {
			return null;
		}
	}

	public function input_id($value = null) {
		echo "<label for=\"i_id\">Id: </label>";
		echo "<input id=\"i_id\" name=\"i_id\" class=\"form-control form-control-sm\" type=\"text\" value=\"" . $value . "\" readonly=\"true\" />";
	}

	public function input_text($field, $label, $value = null, $maxlength = null, $placeholder = null) {
		$error = $this->handle_error ( $field );
		echo "<div class=\"form-group " . ((form_error ( $field )) ? 'has-error' : '') . "\">";
		echo "<label for=\"" . $field . "\">" . $label . ":</label>";
		echo "<input id=\"" . $field . "\" name=\"" . $field . "\" placeholder=\"$placeholder\" class=\"form-control form-control-sm\" type=\"text\" value=\"" . $value . "\" maxlength=\"$maxlength\">";
		echo $error ['span'];
		echo "</div>";
	}
	
	public function input_email($field, $label, $value = null, $maxlength = null, $placeholder = null) {
		$error = $this->handle_error ( $field );
		echo "<div class=\"form-group " . ((form_error ( $field )) ? 'has-error' : '') . "\">";
		echo "<label for=\"" . $field . "\">" . $label . ":</label>";
		echo "<input id=\"" . $field . "\" name=\"" . $field . "\" placeholder=\"$placeholder\" class=\"form-control form-control-sm lowercase\" type=\"text\" value=\"" . $value . "\" maxlength=\"$maxlength\">";
		echo $error ['span'];
		echo "</div>";
	}

	public function input_password($field, $label, $value = null, $maxlength = null) {
		$error = $this->handle_error ( $field );
		echo "<div class=\"form-group " . ((form_error ( $field )) ? 'has-error' : '') . "\">";
		echo "<label for=\"" . $field . "\">" . $label . ":</label>";
		echo "<input id=\"" . $field . "\" name=\"" . $field . "\" class=\"form-control form-control-sm\" type=\"password\" value=\"" . $value . "\" maxlength=\"$maxlength\">";
		echo $error ['span'];
		echo "</div>";
	}

	public function input_cpf($field = 'i_cpf', $label = 'CPF', $value = null) {
		$error = $this->handle_error ( $field );
		echo "<div class=\"form-group " . ((form_error ( $field )) ? 'has-error' : '') . "\">";
		echo "<label for=\"" . $field . "\">" . $label . ":</label>";
		echo "<input id=\"" . $field . "\" name=\"" . $field . "\" class=\"form-control form-control-sm\ cpf\" type=\"text\" value=\"" . $value . "\">";
		echo $error ['span'];
		echo "</div>";
	}

	public function input_textarea($field, $label, $value = null, $rows = 3) {
		$error = $this->handle_error ( $field );
		echo "<div class=\"form-group " . $error ['class'] . "\">";
		echo "<label for=\"" . $field . "\">" . $label . ":</label>";
		echo "<textarea id=\"" . $field . "\" name=\"" . $field . "\" class=\"form-control form-control-sm crsr-cpf\" rows=\"" . $rows . "\">" . $value . "</textarea>";
		echo $error ['span'];
		echo "</div>";
	}

	public function input_datetime2date($field, $label, $value = null) {
		$error = $this->handle_error ( $field );
		echo "<div class=\"form-group " . $error ['class'] . "\">";
		echo "<label for=\"i_" . $field . "\">" . $label . ":</label>";
		echo "<input id=\"i_" . $field . "\" name=\"" . $field . "\" class=\"form-control form-control-sm crsr-datetime2date\" type=\"text\" value=\"" . $value . "\" />";
		echo $error ['span'];
		echo "</div>";
	}

	public function btn_salvar() {
		echo "<br/><input type=\"button\" id=\"btn_salvar\" name=\"btn_salvar\" value=\"Salvar\" class=\"btn btn-primary\" style=\"width: 100%; margin-top: 5px\"/>";
	}

	public function btn($id, $label) {
		echo "<br/><input type=\"button\" id=\"$id\" name=\"$id\" value=\"$label\" class=\"btn btn-primary\" style=\"width: 100%; margin-top: 5px\"/>";
	}

	public function submit($id, $label) {
		echo "<br/><input type=\"submit\" id=\"$id\" name=\"$id\" value=\"$label\" class=\"btn btn-primary\" style=\"width: 100%; margin-top: 5px\"/>";
	}

	public function alert_box($type) {
		switch ($type) {
			case "info" :
				$flashdata_key = 'form_msg_info';
				$bootstrap_class = 'alert-success';
				$titulo = "Informação";
				break;
			case "error" :
				$flashdata_key = 'form_msg_error';
				$bootstrap_class = 'alert-danger';
				$titulo = "Erro!";
				break;
			case "warn" :
				$flashdata_key = 'form_msg_warn';
				$bootstrap_class = 'alert-warning';
				$titulo = "Atenção!";
				break;
			default :
				return;
		}
		
		if ($this->session->flashdata ( $flashdata_key )) {
			echo "<div class=\"alert $bootstrap_class alert-dismissible\" role=\"alert\">";
			echo "<button type=\"button\" class=\"close\" data-dismiss=\"alert\" aria-label=\"Close\">";
			echo "<span aria-hidden=\"true\">&times;</span>";
			echo "</button>";
			echo "<strong>" . $titulo . "</strong><br />";
			echo $this->session->flashdata ( $flashdata_key );
			if ($this->session->flashdata ( 'db_error_msg' )) {
				echo "<p>" . $this->session->flashdata ( 'db_error_msg' ) . "</p>";
			}
			echo "</div>";
		}
	}

	public function spacer($px) {
		echo "<img height=\"" . $px . "px\" src=\"" . base_url ( 'images/dot_clear.gif' ) . "\" />";
	}
}
