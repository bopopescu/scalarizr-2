
import sys

from scalarizr import storage2
from scalarizr.libs import bases


class Error(Exception):
	pass


backup_types = {}
restore_types = {}


def backup(*args, **kwds):
	if args:
		if isinstance(args[0], dict):
			return backup(**args[0])
		else:
			return args[0]
	type_ = kwds.get('type', 'base')
	try:
		cls = backup_types[type_]
	except KeyError:
		msg = "Unknown backup type '%s'. "
		"Have you registered it in "
		"scalarizr.services.backup.backup_types?" % type_
		raise Error(msg)
	return cls(**kwds)


def restore(*args, **kwds):
	if args:
		if isinstance(args[0], dict):
			return restore(**args[0])
		else:
			return args[0]
	type_ = kwds.get('type', 'base')
	try:
		cls = restore_types[type_]
	except KeyError:
		msg = "Unknown restore type '%s'. "
		"Have you registered it in " 
		"scalarizr.services.backup.restore_types?" % type_
		raise Error(msg)
	return cls(**kwds)


class Backup(bases.Task):
	features = {
		'start_slave': True
	}

	def __init__(self, 
				type='base', 
				description=None, 
				tags=None,
				**kwds):
		super(Backup, self).__init__(
				type=type,
				description=description,
				tags=tags or {},
				**kwds)


class Restore(bases.Task):

	features = {
		'master_binlog_reset': False
	}
	'''
	When 'master_binlog_reset' = False, 
	rolling this restore on Master causes replication binary log reset. 
	Slaves should start from the binary log head. Detecting the first 
	position in binary log is implementation dependent and Master is 
	responsible for this.
	'''

	def __init__(self, 
				type='base', 
				**kwds):
		super(Backup, self).__init__(
				type=type,
				**kwds)


backup_types['base'] = Backup
restore_types['base'] = Restore


class SnapBackup(Backup):

	def __init__(self, 
				volume=None, 
				**kwds):
		super(SnapBackup, self).__init__(
				volume=volume, 
				**kwds)
		self.define_events(
			# Fires when all disk I/O activity should be freezed 
			'freeze'   
		)

	def _run(self):
		self.volume = storage2.volume(self.volume)
		state = {}
		self.fire('freeze', self.volume, state)
		snap = self.volume.snapshot(self.description, tags=self.tags)
		return restore(
				type=self.type, 
				snapshot=snap,
				**state)


class SnapRestore(Restore):

	def __init__(self, snapshot=None, volume=None, **kwds):
		super(SnapRestore, self).__init__(
				snapshot=snapshot, 
				volume=volume, 
				**kwds)


	def _run(self):
		self.snapshot = storage2.snapshot(self.snapshot)
		if self.volume:
			self.volume = storage2.volume(self.volume)
			self.volume.snap = self.snapshot
			self.volume.ensure()
		else:
			self.volume = self.snapshot.restore()
		return self.volume
		

backup_types['snap'] = SnapBackup
restore_types['snap'] = SnapRestore



