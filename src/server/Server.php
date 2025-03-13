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

use pocketmine\utils\BinaryDataException;
use raklib\generic\DisconnectReason;
use raklib\generic\PacketHandlingException;
use raklib\generic\Session;
use raklib\generic\SocketException;
use raklib\protocol\ACK;
use raklib\protocol\Datagram;
use raklib\protocol\EncapsulatedPacket;
use raklib\protocol\NACK;
use raklib\protocol\Packet;
use raklib\protocol\PacketSerializer;
use raklib\utils\ExceptionTraceCleaner;
use raklib\utils\InternetAddress;
use function asort;
use function assert;
use function bin2hex;
use function count;
use function get_class;
use function microtime;
use function ord;
use function preg_match;
use function strlen;
use function time;
use function time_sleep_until;
use const PHP_INT_MAX;
use const SOCKET_ECONNRESET;

class Server implements ServerInterface{

	private const RAKLIB_TPS = 100;
	private const RAKLIB_TIME_PER_TICK = 1 / self::RAKLIB_TPS;

	protected int $receiveBytes = 0;
	protected int $sendBytes = 0;

	/** @var ServerSession[] */
	protected array $sessionsByAddress = [];
	/** @var ServerSession[] */
	protected array $sessions = [];

	protected UnconnectedMessageHandler $unconnectedMessageHandler;

	protected string $name = "";

	protected int $packetLimit = 200;

	protected bool $shutdown = false;

	protected int $ticks = 0;

	/** @var int[] string (address) => int (unblock time) */
	protected array $block = [];
	/** @var int[] string (address) => int (number of packets) */
	protected array $ipSec = [];

	/** @var string[] regex filters used to block out unwanted raw packets */
	protected array $rawPacketFilters = [];

	public bool $portChecking = false;

	protected int $nextSessionId = 0;

	/**
	 * @phpstan-param positive-int $recvMaxSplitParts
	 * @phpstan-param positive-int $recvMaxConcurrentSplits
	 */
	public function __construct(
		protected int $serverId,
		protected \Logger $logger,
		protected ServerSocket $socket,
		protected int $maxMtuSize,
		ProtocolAcceptor $protocolAcceptor,
		private ServerEventSource $eventSource,
		private ServerEventListener $eventListener,
		private ExceptionTraceCleaner $traceCleaner,
		private int $recvMaxSplitParts = ServerSession::DEFAULT_MAX_SPLIT_PART_COUNT,
		private int $recvMaxConcurrentSplits = ServerSession::DEFAULT_MAX_CONCURRENT_SPLIT_COUNT
	){
		if($maxMtuSize < Session::MIN_MTU_SIZE){
			throw new \InvalidArgumentException("MTU size must be at least " . Session::MIN_MTU_SIZE . ", got $maxMtuSize");
		}
		$this->socket->setBlocking(false);

		$this->unconnectedMessageHandler = new UnconnectedMessageHandler($this, $protocolAcceptor);
	}

	public function getPort() : int{
		return $this->socket->getBindAddress()->getPort();
	}

	public function getMaxMtuSize() : int{
		return $this->maxMtuSize;
	}

	public function getLogger() : \Logger{
		return $this->logger;
	}

	public function tickProcessor() : void{
		$start = microtime(true);

		/*
		 * The below code is designed to allow co-op between sending and receiving to avoid slowing down either one
		 * when high traffic is coming either way. Yielding will occur after 100 messages.
		 */
		do{
			$stream = !$this->shutdown;
			for($i = 0; $i < 100 && $stream && !$this->shutdown; ++$i){ //if we received a shutdown event, we don't care about any more messages from the event source
				$stream = $this->eventSource->process($this);
			}

			$socket = true;
			for($i = 0; $i < 100 && $socket; ++$i){
				$socket = $this->receivePacket();
			}
		}while($stream || $socket);

		$this->tick();

		$time = microtime(true) - $start;
		if($time < self::RAKLIB_TIME_PER_TICK){
			@time_sleep_until(microtime(true) + self::RAKLIB_TIME_PER_TICK - $time);
		}
	}

	/**
	 * Disconnects all sessions and blocks until everything has been shut down properly.
	 */
	public function waitShutdown() : void{
		$this->shutdown = true;

		while($this->eventSource->process($this)){
			//Ensure that any late messages are processed before we start initiating server disconnects, so that if the
			//server implementation used a custom disconnect mechanism (e.g. a server transfer), we don't break it in
			//race conditions.
		}

		foreach($this->sessions as $session){
			$session->initiateDisconnect(DisconnectReason::SERVER_SHUTDOWN);
		}

		while(count($this->sessions) > 0){
			$this->tickProcessor();
		}

		$this->socket->close();
		$this->logger->debug("Graceful shutdown complete");
	}

	private function tick() : void{
		$time = microtime(true);
		// check for any new tcp connection
		$this->acceptNewConnections();

		// handle packets from the client
		while($this->receivePacket()){
			// prevent from high load situations
			if(--$this->packetLimit <= 0){
				break;
			}
		}
	
		// process command queue
		while($this->eventSource->process()){
			// not needed
		}

		// update sessions
		foreach($this->sessions as $session){
			$session->update($time);
		}

		// Process IP block timeouts
		if(count($this->block) > 0){
			$now = time();
			foreach($this->block as $address => $timeout){
				if($timeout <= $now){
					unset($this->block[$address]);
				}
			}
		}

				// process IP security (rate limiting)
		if(count($this->ipSec) > 0){
			$now = time();
			if($now - $this->ticks >= 1){
				$this->ipSec = [];
				$this->ticks = $now;
			}
		}
	}

    /**
	 * accepts new TCP connections
	 */
	private function acceptNewConnections() : void{
		// accept multiple pending connections per tick
		$maxNewConnectionsPerTick = 4;
		for($i = 0; $i < $maxNewConnectionsPerTick; $i++){
			try{
				$result = $this->socket->acceptNewConnection();
				if($result === null){
					break;
				}
				
				[$clientSocket, $address] = $result;
				$this->logger->debug("New TCP connection from $address");
				
				// new TCP connections need to go through the RakNet connection sequence
				// they should send an ID_OPEN_CONNECTION_REQUEST_1 as their first packet
				// we don't create a session yet, we wait for the proper handshake
				
			}catch(SocketException $e){
				$this->logger->debug("Failed to accept connection: " . $e->getMessage());
				break;
			}
		}
	}

				/** @phpstan-impure */
	private function receivePacket() : bool{
		// Get all connected clients
		$connectedClients = $this->socket->getConnectedClients();
		if(count($connectedClients) === 0){
			return false;
		}
		
		// Try to read from each client until we get data
		foreach($connectedClients as $address){
			try{
				$buffer = $this->socket->readPacket($address);
				if($buffer === null){
					continue; 
				}
				
				$len = strlen($buffer);
				$this->receiveBytes += $len;
				
				// Check for IP blocking
				$addressIp = $address->getIp();
				if(isset($this->block[$addressIp])){
					continue;
				}
				
				// Rate limiting
				if(isset($this->ipSec[$addressIp])){
					if(++$this->ipSec[$addressIp] >= $this->packetLimit){
						$this->blockAddress($addressIp);
						continue;
					}
				}else{
					$this->ipSec[$addressIp] = 1;
				}
				
				if($len < 1){
					continue;
				}
				
				// Process the packet
				try{
					$session = $this->getSessionByAddress($address);
					if($session !== null){
						$header = ord($buffer[0]);
						if(($header & Datagram::BITFLAG_VALID) !== 0){
							if(($header & Datagram::BITFLAG_ACK) !== 0){
								$packet = new ACK();
							}elseif(($header & Datagram::BITFLAG_NAK) !== 0){
								$packet = new NACK();
							}else{
								$packet = new Datagram();
							}
							$packet->decode(new PacketSerializer($buffer));
							$session->handlePacket($packet);
						}else{
							$this->logger->debug("Received invalid packet from $address");
						}
					}else{
						// new connection or unconnected message
						if(!$this->unconnectedMessageHandler->handleRaw($buffer, $address)){
							$this->logger->debug("Dropping unhandled unconnected packet from $address: " . bin2hex($buffer));
						}
					}
					return true;
				}catch(BinaryDataException $e){
					$this->logger->debug("Packet from $address (" . strlen($buffer) . " bytes): " . bin2hex($buffer));
					$this->logger->debug(get_class($e) . ": " . $e->getMessage() . " in " . $this->traceCleaner->getCleanTraceAsString($e->getTrace()));
				}catch(PacketHandlingException $e){
					$this->logger->debug(get_class($e) . ": " . $e->getMessage() . " in " . $this->traceCleaner->getCleanTraceAsString($e->getTrace()));
				}
			}catch(SocketException $e){
				$error = $e->getCode();
				if($error === SOCKET_ECONNRESET){
					// client disconnected improperly handled by the socket class
					continue;
				}

				
				$this->logger->debug($e->getMessage());
			}

		return false;
	}

	public function sendPacket(Packet $packet, InternetAddress $address) : void{
		try{
			$out = new PacketSerializer();
			$packet->encode($out);
			
			// Only send if client is connected
			if($this->socket->hasClient($address)){
				$this->socket->writePacket($out->getBuffer(), $address);
				$this->sendBytes += strlen($out->getBuffer());
			}
		}catch(SocketException $e){
			$this->logger->debug("Failed to send packet to $address: " . $e->getMessage());
		}
	}

	public function getEventListener() : ServerEventListener{
		return $this->eventListener;
	}

	public function sendEncapsulated(int $sessionId, EncapsulatedPacket $packet, bool $immediate = false) : void{
		$session = $this->sessions[$sessionId] ?? null;
		if($session !== null and $session->isConnected()){
			$session->addEncapsulatedToQueue($packet, $immediate);
		}
	}

	public function sendRaw(string $address, int $port, string $payload) : void{
		try{
			$internetAddress = new InternetAddress($address, $port, $this->socket->getBindAddress()->getVersion());
			
			// only send if client is connected
			if($this->socket->hasClient($internetAddress)){
				$this->socket->writePacket($payload, $internetAddress);
				$this->sendBytes += strlen($payload);
			}
		}catch(SocketException $e){
			$this->logger->debug("Failed to send raw packet to $address:$port: " . $e->getMessage());
		}
	}

	public function closeSession(int $sessionId) : void{
		if(isset($this->sessions[$sessionId])){
			$this->sessions[$sessionId]->initiateDisconnect(DisconnectReason::SERVER_DISCONNECT);
		}
	}

	public function setName(string $name) : void{
		$this->name = $name;
	}

	public function setPortCheck(bool $value) : void{
		$this->portChecking = $value;
	}

	public function setPacketsPerTickLimit(int $limit) : void{
		$this->packetLimit = $limit;
	}

	public function blockAddress(string $address, int $timeout = 300) : void{
		$final = time() + $timeout;
		if(!isset($this->block[$address]) or $timeout === -1){
			if($timeout === -1){
				$final = PHP_INT_MAX;
			}else{
				$this->logger->notice("Blocked $address for $timeout seconds");
			}
			$this->block[$address] = $final;
		}elseif($this->block[$address] < $final){
			$this->block[$address] = $final;
		}
	}

	public function unblockAddress(string $address) : void{
		unset($this->block[$address]);
		$this->logger->debug("Unblocked $address");
	}

	public function addRawPacketFilter(string $regex) : void{
		$this->rawPacketFilters[] = $regex;
	}

	public function getSessionByAddress(InternetAddress $address) : ?ServerSession{
		return $this->sessionsByAddress[$address->toString()] ?? null;
	}

	public function sessionExists(InternetAddress $address) : bool{
		return isset($this->sessionsByAddress[$address->toString()]);
	}

	public function createSession(InternetAddress $address, int $clientId, int $mtuSize) : ServerSession{
		$existingSession = $this->sessionsByAddress[$address->toString()] ?? null;
		if($existingSession !== null){
			$existingSession->forciblyDisconnect(DisconnectReason::CLIENT_RECONNECT);
			$this->removeSessionInternal($existingSession);
		}

		$this->checkSessions();

		while(isset($this->sessions[$this->nextSessionId])){
			$this->nextSessionId++;
			$this->nextSessionId &= 0x7fffffff; //we don't expect more than 2 billion simultaneous connections, and this fits in 4 bytes
		}

		$session = new ServerSession($this, $this->logger, clone $address, $clientId, $mtuSize, $this->nextSessionId, $this->recvMaxSplitParts, $this->recvMaxConcurrentSplits);
		$this->sessionsByAddress[$address->toString()] = $session;
		$this->sessions[$this->nextSessionId] = $session;
		$this->logger->debug("Created session for $address with MTU size $mtuSize");

		return $session;
	}

	private function removeSessionInternal(ServerSession $session) : void{
		unset($this->sessionsByAddress[$session->getAddress()->toString()], $this->sessions[$session->getInternalId()]);
	}

	public function openSession(ServerSession $session) : void{
		$address = $session->getAddress();
		$this->eventListener->onClientConnect($session->getInternalId(), $address->getIp(), $address->getPort(), $session->getID());
	}

	private function checkSessions() : void{
		if(count($this->sessions) > 4096){
			foreach($this->sessions as $sessionId => $session){
				if($session->isTemporary()){
					$this->removeSessionInternal($session);
					if(count($this->sessions) <= 4096){
						break;
					}
				}
			}
		}
	}

	public function getName() : string{
		return $this->name;
	}

	public function getID() : int{
		return $this->serverId;
	}
}
