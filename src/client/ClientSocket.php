<?php

/*
 * This file is part of RakLib.
 * Copyright (C) 2014-2022 PocketMine Team <https://github.com/pmmp/RakLib>
 *
 * RakLib is not affiliated with Jenkins Software LLC nor RakNet.
 *
 * RakLib is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

declare(strict_types=1);

namespace raklib\client;

use raklib\generic\Socket;
use raklib\generic\SocketException;
use raklib\utils\InternetAddress;
use function socket_connect;
use function socket_last_error;
use function socket_recv;
use function socket_send;
use function socket_strerror;
use function strlen;
use function trim;

class ClientSocket extends Socket{
	/** @var string Buffer for partial packets */
	private string $receiveBuffer = "";
	
	/** @var bool Whether the socket is connected */
	private bool $isConnected = false;

	public function __construct(
		private InternetAddress $connectAddress
	){
		parent::__construct($this->connectAddress->getVersion() === 6);
		$this->connect();
	}
	
	/**
	 * Connect to the server
	 * 
	 * @throws SocketException
	 */
	public function connect() : void {
		if($this->isConnected){
			return;
		}
		
		if(!@socket_connect($this->socket, $this->connectAddress->getIp(), $this->connectAddress->getPort())){
			$error = socket_last_error($this->socket);
			throw new SocketException("Failed to connect to " . $this->connectAddress . ": " . trim(socket_strerror($error)), $error);
		}
		
		$this->isConnected = true;
		//TODO: is an 8 MB buffer really appropriate for a client??
		$this->setSendBuffer(1024 * 1024 * 8)->setRecvBuffer(1024 * 1024 * 8);
	}
	
	/**
	 * Reconnect to the server
	 * 
	 * @throws SocketException
	 */
	public function reconnect() : void {
		$this->close();
		
		// Recreate the socket
		$this->__construct($this->connectAddress);
	}

	public function getConnectAddress() : InternetAddress{
		return $this->connectAddress;
	}
	
	/**
	 * Check if the client is connected
	 */
	public function isConnected() : bool {
		return $this->isConnected;
	}

	/**
	 * @throws SocketException
	 */
	public function readPacket() : ?string{
		$recvBuffer = "";
		$result = @socket_recv($this->socket, $recvBuffer, 65535, 0);
		
		if($result === false){
			$errno = socket_last_error($this->socket);
			if($errno === SOCKET_EWOULDBLOCK){
				return null;
			}elseif($errno === SOCKET_ECONNRESET || $errno === SOCKET_ENOTCONN){
				// Connection closed by server
				$this->isConnected = false;
				throw new SocketException("Connection closed by server", $errno);
			}
			throw new SocketException("Failed to recv (errno $errno): " . trim(socket_strerror($errno)), $errno);
		}elseif($result === 0){
			// Connection closed gracefully
			$this->isConnected = false;
			throw new SocketException("Connection closed gracefully by server", 0);
		}
		
		// Append received data to the buffer
		$this->receiveBuffer .= $recvBuffer;
		
		// Check if we have data to return
		if(strlen($this->receiveBuffer) > 0){
			$packet = $this->receiveBuffer;
			$this->receiveBuffer = "";
			return $packet;
		}
		
		return null;
	}

	/**
	 * @throws SocketException
	 */
	public function writePacket(string $buffer) : int{
		if(!$this->isConnected){
			throw new SocketException("Cannot send data: Not connected", 0);
		}
		
		$result = @socket_send($this->socket, $buffer, strlen($buffer), 0);
		if($result === false){
			$errno = socket_last_error($this->socket);
			if($errno === SOCKET_ECONNRESET || $errno === SOCKET_ENOTCONN){
				$this->isConnected = false;
				throw new SocketException("Failed to send packet: Connection closed", $errno);
			}
			throw new SocketException("Failed to send packet (errno $errno): " . trim(socket_strerror($errno)), $errno);
		}
		return $result;
	}
	
	/**
	 * Close the connection
	 */
	public function close() : void {
		$this->isConnected = false;
		parent::close();
	}
}
