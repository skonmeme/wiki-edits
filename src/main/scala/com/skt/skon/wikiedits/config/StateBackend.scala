package com.skt.skon.wikiedits.config

sealed trait StateBackend

case class NoStateBackend() extends StateBackend
case class MemoryStateBackend() extends StateBackend
case class FsStateBackend(uri: String) extends StateBackend
case class RocksDBStateBackend(uri: String) extends StateBackend
