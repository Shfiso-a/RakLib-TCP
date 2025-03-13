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

namespace raklib\server;

use raklib\generic\Socket;
use raklib\generic\SocketException;
use raklib\utils\InternetAddress;
use function socket_accept;
use function socket_bind;
use function socket_getpeername;
use function socket_last_error;
use function socket_listen;
use function socket_recv;
use function socket_send;
use function socket_set_option;
use function socket_strerror;
use function strlen;
use function trim;

class ServerSocket extends Socket{
	/** @var \Socket[] Array of connected client sockets where keys are string addresses */
	private array $clientSockets = [];
	
	/** @var string[] Buffers for incomplete packets, keys are string addresses */
	private array $receiveBuffers = [];
	
	/** @var int Max number of pending connections in the queue */
	private int $backlog = 128;

	public function __construct(
		private InternetAddress $bindAddress
	){
		parent::__construct($this->bindAddress->getVersion() === 6);

		if(@socket_bind($this->socket, $this->bindAddress->getIp(), $this->bindAddress->getPort()) === true){
			// TCP specific - start listening for connections
			if(@socket_listen($this->socket, $this->backlog) === false){
				$error = socket_last_error($this->socket);
				throw new SocketException("Failed to start listening on socket: " . trim(socket_strerror($error)), $error);
			}
			
			$this->setSendBuffer(1024 * 1024 * 8)->setRecvBuffer(1024 * 1024 * 8);
		}else{
			$error = socket_last_error($this->socket);
			if($error === SOCKET_EADDRINUSE){ //platform error messages aren't consistent
				throw new SocketException("Failed to bind socket: Something else is already running on $this->bindAddress", $error);
			}
			throw new SocketException("Failed to bind to " . $this->bindAddress . ": " . trim(socket_strerror($error)), $error);
		}
	}

	public function getBindAddress() : InternetAddress{
		return $this->bindAddress;
	}

	public function enableBroadcast() : bool{
		// Broadcasting concept doesn't apply to TCP, but keep method for API compatibility
		return true;
	}

	public function disableBroadcast() : bool{
		// Broadcasting concept doesn't apply to TCP, but keep method for API compatibility
		return true;
	}

	/**
	 * Accept a new client connection
	 * 
	 * @return array{0: \Socket, 1: InternetAddress}|null Returns [client socket, address] or null if no connection pending
	 * @throws SocketException
	 */
	public function acceptNewConnection() : ?array{
		$clientSocket = @socket_accept($this->socket);
		
		if($clientSocket === false){
			$errno = socket_last_error($this->socket);
			if($errno === SOCKET_EWOULDBLOCK){
				return null;
			}
			throw new SocketException("Failed to accept connection (errno $errno): " . trim(socket_strerror($errno)), $errno);
		}
		
		// Get client's address info
		$address = '';
		$port = 0;
		socket_getpeername($clientSocket, $address, $port);
		
		// Create InternetAddress object based on the connection info
		$internetAddress = new InternetAddress($address, $port, $this->bindAddress->getVersion());
		
		// Store the client socket
		$addressString = $internetAddress->toString();
		$this->clientSockets[$addressString] = $clientSocket;
		$this->receiveBuffers[$addressString] = "";
		
		return [$clientSocket, $internetAddress];
	}
	
	/**
	 * Reads data from a specific client
	 * 
	 * @param InternetAddress $address The client's address
	 * @throws SocketException
	 */
	public function readPacket(InternetAddress $address) : ?string{
		$addressString = $address->toString();
		
		if(!isset($this->clientSockets[$addressString])){
			return null;
		}
		
		$clientSocket = $this->clientSockets[$addressString];
		
		// Try to receive data
		$recvBuffer = "";
		$result = @socket_recv($clientSocket, $recvBuffer, 65535, 0);
		
		if($result === false){
			$errno = socket_last_error($clientSocket);
			if($errno === SOCKET_EWOULDBLOCK){
				return null;
			}elseif($errno === SOCKET_ECONNRESET || $errno === SOCKET_ENOTCONN){
				// Connection closed by client
				$this->removeClient($address);
				return null;
			}
			throw new SocketException("Failed to recv from client {$address} (errno $errno): " . trim(socket_strerror($errno)), $errno);
		}elseif($result === 0){
			// Connection closed gracefully
			$this->removeClient($address);
			return null;
		}
		
		// Append received data to the buffer for this client
		$this->receiveBuffers[$addressString] .= $recvBuffer;
		
		// Check if we have a complete packet
		// Since RakNet packets typically have length fields, we'll look for at least 1 byte
		// and return whatever we have. The Protocol implementation is responsible for handling
		// incomplete packets.
		if(strlen($this->receiveBuffers[$addressString]) > 0){
			$packet = $this->receiveBuffers[$addressString];
			$this->receiveBuffers[$addressString] = "";
			return $packet;
		}
		
		return null;
	}

	/**
	 * Writes a packet to a specific client
	 * 
	 * @throws SocketException
	 */
	public function writePacket(string $buffer, InternetAddress $address) : int{
		$addressString = $address->toString();
		
		if(!isset($this->clientSockets[$addressString])){
			throw new SocketException("Cannot send to $address: not connected", 0);
		}
		
		$clientSocket = $this->clientSockets[$addressString];
		$result = @socket_send($clientSocket, $buffer, strlen($buffer), 0);
		
		if($result === false){
			$errno = socket_last_error($clientSocket);
			if($errno === SOCKET_ECONNRESET || $errno === SOCKET_ENOTCONN){
				// Connection closed by client
				$this->removeClient($address);
				throw new SocketException("Failed to send to $address: Connection closed", $errno);
			}
			throw new SocketException("Failed to send to $address (errno $errno): " . trim(socket_strerror($errno)), $errno);
		}
		
		return $result;
	}
	
	/**
	 * Remove a client connection
	 */
	public function removeClient(InternetAddress $address) : void{
		$addressString = $address->toString();
		
		if(isset($this->clientSockets[$addressString])){
			socket_close($this->clientSockets[$addressString]);
			unset($this->clientSockets[$addressString]);
			unset($this->receiveBuffers[$addressString]);
		}
	}
	
	/**
	 * Check if a client is connected
	 */
	public function hasClient(InternetAddress $address) : bool{
		return isset($this->clientSockets[$address->toString()]);
	}
	
	/**
	 * Get all connected client addresses
	 * 
	 * @return InternetAddress[]
	 */
	public function getConnectedClients() : array{
		$addresses = [];
		foreach(array_keys($this->clientSockets) as $addressString){
			[$ip, $port] = explode(":", $addressString);
			$addresses[] = new InternetAddress($ip, (int)$port, $this->bindAddress->getVersion());
		}
		return $addresses;
	}
	
	/**
	 * Close all client connections
	 */
	public function closeAllClients() : void{
		foreach($this->clientSockets as $socket){
			socket_close($socket);
		}
		$this->clientSockets = [];
	}
	
	/**
	 * Close the server socket and all client connections
	 */
	public function close() : void{
		$this->closeAllClients();
		parent::close();
	}
}
