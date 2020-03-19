<?php

use PHPMailer\PHPMailer\PHPMailer;
use PHPMailer\PHPMailer\SMTP;
use PHPMailer\PHPMailer\Exception;

// Load Composer's autoloader
require 'vendor/autoload.php';

class LogWriter
{

	private $fh;

	private $fileName;

	private $logFile;

	private $debug;

	private $info;

	/**
	 * INFO ou DEBUG.
	 *
	 * @var string
	 */
	private $level = 'INFO';

	public function __construct($logPath, $prefix = null)
	{
		$this->openLog($logPath, $prefix);
	}

	public function setLevel($level)
	{
		$this->level = $level;
	}

	public function getLevel()
	{
		return $this->level;
	}

	private function openLog($logPath, $prefix = null)
	{
		$agora = new DateTime();

		$this->fileNameInfo = $prefix . $agora->format('Y-m-d_H-i-s') . ".info.log";
		$this->logFileInfo = $logPath . '/' . $this->fileNameInfo;
		$this->fhInfo = fopen($this->logFileInfo, 'a') or die("Impossível abrir arquivo de log: [" . $this->logFileInfo . "]" . PHP_EOL);

		$this->fileNameDebug = $prefix . $agora->format('Y-m-d_H-i-s') . ".debug.log";
		$this->logFileDebug = $logPath . '/' . $this->fileNameDebug;
		$this->fhDebug = fopen($this->logFileDebug, 'a') or die("Impossível abrir arquivo de log: [" . $this->logFileDebug . "]" . PHP_EOL);
	}

	public function debug($msg, $echoo = true)
	{
		$msg .= PHP_EOL;
		if ($echoo) {
			echo $msg;
		}
		fwrite($this->fhDebug, $msg);
	}

	public function info($msg, $echoo = true)
	{
		$msg .= PHP_EOL;
		if ($echoo)
			echo $msg;
		fwrite($this->fhInfo, $msg);
		fwrite($this->fhDebug, $msg);
	}

	public function closeLog()
	{
		if (is_resource($this->fhInfo)) {
			fclose($this->fhInfo);
		}
		if (is_resource($this->fhDebug)) {
			fclose($this->fhDebug);
		}
	}

	public function sendMail($level = 'INFO')
	{
		$props = parse_ini_file('.env');

		try {
			$mail = new PHPMailer(true);
			$mail->isSMTP();
			$mail->Host = 'smtp.ektplus.com.br';
			$mail->SMTPAuth = true;
			$mail->Port = 587;
			$mail->SMTPSecure = false;
			$mail->SMTPAutoTLS = false;
			$mail->Username = 'mailer@ektplus.com.br';
			$mail->Password = $props['PWDMAILER'];
			$mail->setFrom('mailer@ektplus.com.br');
			$mail->addAddress('carlospauluk@gmail.com');
			//$mail->addAddress('ekt@ekt.com.br');
			$mail->addReplyTo('mailer@ektplus.com.br');
			$mail->isHTML(true);
			$mail->Subject = "RESULT: " . $this->fileNameInfo;
			$mail->Body = file_get_contents($level == 'INFO' ? $this->logFileInfo : $this->logFileDebug);
			$mail->send();
			unset($_POST);
			return [
				'status' => 'OK',
				'msg' => 'Ocorreu um erro ao enviar sua mensagem. Por favor, envie um e-mail diretamente para ekt@ektplus.com.br'
			];
		} catch (Exception $e) {
			return [
				'status' => 'ERRO',
				'msg' => 'Ocorreu um erro ao enviar sua mensagem. Por favor, envie um e-mail diretamente para ekt@ektplus.com.br'
			];
		}
	}
}
