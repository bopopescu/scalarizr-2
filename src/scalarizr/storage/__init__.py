
from .lvm2 import Lvm2
from .raid import Mdadm
from .fs import MOUNT_PATH, UMOUNT_PATH, SYNC_PATH

from scalarizr.util import system2, PopenError, firstmatched
from scalarizr.libs.metaconf import Configuration

import urlparse
import subprocess
from random import randint
import hashlib
import logging
import os
import re
import time
import glob
from scalarizr.util.filetool import read_file


# ebs-raid0-lvm-ext3
# ebs-raid0-xfs
# eph-lvm-ext3
# eph-xfs


'''
# ebs-raid0-lvm-ext3

vol = mgr.create_raid(devices=('/dev/ebs1', '/dev/ebs2'), level='striping')
vol = mgr.lvm_wrapper(vol, buffer_device='/dev/loop0')
vol.mkfs('ext3')
snap = vol.snapshot()

# ebs-raid0-xfs
vol = mgr.create_raid(devices=('/dev/ebs1', '/dev/ebs2'), level='striping')
vol.mkfs('xfs')
snap = vol.snapshot()


# eph-lvm-ext3
vol = mgr.create_reliable_volume(device='/dev/sdb', shadow='/dev/sdd', 
		snapshot_backend=CloudfilesSnapshotBackend('/container/key-basename')
vol = mgr.lvm_wrapper(vol, buffer_device='/dev/loop0')
vol.mkfs('ext3')
snap = vol.snapshot()


# eph-xfs
vol = mgr.create_reliable_volume(device='/dev/loop0', shadow='/dev/loop1', 
		snapshot_backend=CloudfilesSnapshotBackend('/container/key-basename')
vol.mkfs('xfs')
snap = vol.snapshot()

lvm = Lvm2()
vg = lvm.create_vg(None, devices=('/dev/loop0',), ph_extent_size=16)
lv1 = lvm.create_lv(vg, 'data', size=10)
lv2 = lvm.create_lv(vg, 'backup', size=10)

mgr.create_ephs(ph_device='/dev/loop', lv_ratio=(40, 40, 20), vg_name=None, vg_options=None, snap_backend=CloudfilesSnapshotBackend())
mgr.create_ephs(lv_data=lv1, lv_backup=lv2, snap_size=5, snap_backend=CloudfilesSnapshotBackend())

lvm = Lvm2()
mdadm = Mdadm()
md = mdadm.create(('/dev/sdebs1', '/dev/sdebs2'), level=0)
vg = lvm.create_vg(None, devices=md)
lv = lvm.create_lv(vg, num_extents='100%FREE')


'''

logger = logging.getLogger(__name__)


def system(*popenargs, **kwargs):
	kwargs['logger'] = logger
	kwargs['exc_class'] = StorageError
	return system2(*popenargs, **kwargs)

class StorageError(PopenError):
	pass


def mkloop(filename, size=None, quick=False):
	''' Create loop device '''
	if size:
		cmd = ['dd', 'if=/dev/zero', 'of=%s' % filename, 'bs=1M']
		if quick:
			cmd.extend(['seek=%d' % (size - 1,), 'count=1'])
		else:
			cmd.extend(['count=%d' % size])
		system(cmd)
	devname = system(('/sbin/losetup', '-f'))[0].strip()
	system(('/sbin/losetup', devname, filename))
	return devname

def listloop():
	# TODO
	pass

def rmloop(device):
	# TODO
	pass

class ResourceMgr:
	@staticmethod
	def lookup_snapshot_backend(scheme):
		self = ResourceMgr
		if scheme in self._snap_backends:
			return self._snap_backends[scheme]
		raise LookupError('Unknown snapshot backend for schema %s://' % scheme)
	
	@staticmethod
	def explore_snapshot_backend(schema, BackendClass):
		self = ResourceMgr
		self._snap_backends[schema] = BackendClass()
	
	@staticmethod
	def lookup_filesystem(fstype):
		self = ResourceMgr
		if fstype in self._fs_drivers:
			return self._fs_drivers[fstype]
		try:
			mod = __import__('scalarizr.storage.fs.%s' % fstype, globals(), locals(), ('__filesystem__',), -1)
			self._fs_drivers[fstype] = mod.__filesystem__()
			return self.lookup_filesystem(fstype)
		except ImportError:
			raise LookupError('Unknown filesystem %s' % fstype)

	@staticmethod
	def explore_filesystem(fstype, FileSystemClass):
		self = ResourceMgr
		self._fs_drivers[fstype] = FileSystemClass()

	@staticmethod
	def reset():
		self = ResourceMgr
		self._fs_drivers = {}
		self._snap_backends = {}
		
ResourceMgr.reset()

class Storage:
	_lvm = None
	
	@staticmethod
	def _init_lvm():
		self = Storage
		if not self._lvm:
			self._lvm = Lvm2()
	
	@staticmethod
	def create_ephs(device, vg_name, vg_options=None, 
				lv_extents='40%VG', snap_pvd=None, snap_backend=None, mpoint=None, fstype=None):
		self = Storage
		self._init_lvm()
		
		# Create VG
		vg_options = vg_options or dict()
		self._lvm.create_pv(device)
		vg = self._lvm.create_vg(vg_name, (device,), **vg_options)
		
		# Create data volume and tranzit volume with the same same
		data_lv = self._lvm.create_lv(vg, 'data', extents=lv_extents)

		# Create tranzit volume (should be 5% bigger then data vol)
		size_in_KB = int(read_file('/sys/block/%s/size' % os.path.basename(os.readlink(data_lv)))) / 2
		tranzit_lv = self._lvm.create_lv(vg, 'tranzit', size='%dK' % size_in_KB*1.05)
		
		# Init snapshot provider 
		snap_pvd = snap_pvd or EphSnapshotProvider()
		
		# Construct storage volume
		return EphVolume(data_lv, mpoint, fstype, vg, tranzit_lv, snap_pvd, snap_backend)

	@staticmethod
	def remove_ephs(vol):
		self = Storage
		self._init_lvm()
		
		# Umount volumes
		vol.umount()
		vol.tranzit_vol.umount()
		
		# Find PV 
		pv = None
		pvi = firstmatched(lambda pvi: vol.vg in pvi.vg, self._lvm.pv_status())
		if pvi:
			pv = pvi.pv
			
		# Remove storage VG
		self._lvm.change_lv(vol.devname, available=False)
		self._lvm.change_lv(vol.tranzit_vol.devname, available=False)
		self._lvm.remove_vg(vol.vg)
		
		if pv:
			# Remove PV if it doesn't belongs to any other VG
			pvi = self._lvm.pv_info(pv)
			if not pvi.vg:
				self._lvm.remove_pv(pv)
				
				
	@staticmethod
	def create_raid(devices, level):
		pass

	@staticmethod
	def remove_raid(vol):
		pass
		
def _fs_should_be_set(f):
	def d(*args):
		if args[0]._fs is None:
			raise StorageError('Filesystem is not set')
		return f(*args)
	return d

class Volume:
	devname = None
	mpoint = None

	_logger = None
	_fs = None

	def __init__(self, devname, mpoint=None, fstype=None):
		self._logger = logging.getLogger(__name__)
		self.devname = devname
		self.mpoint = mpoint
		self.fstype = fstype

	def _set_fstype(self, fstype):
		self._fs = ResourceMgr.lookup_filesystem(fstype)

	def _get_fstype(self):
		return self._fs.name if self._fs else None

	fstype = property(_get_fstype, _set_fstype)

	def mkfs(self, fstype=None):
		fstype = fstype or self.fstype
		if not fstype:
			raise ValueError('Filesystem cannot be None')
		fs = ResourceMgr.lookup_filesystem(fstype) 
		fs.mkfs(self.devname)
		self.fstype = fstype
		self._fs = fs
	
	@_fs_should_be_set
	def resize(self, size=None, **fsargs):
		fsargs = fsargs or dict()
		return self._fs.resize(self.devname, **fsargs)
	
	@_fs_should_be_set
	def _get_label(self):
		return self._fs.get_label(self.devname)
	
	@_fs_should_be_set
	def _set_label(self, lbl):
		self._fs.set_label(self.devname, lbl)
		
	label = property(_get_label, _set_label)
	
	@_fs_should_be_set
	def freeze(self):
		return self._fs.freeze(self.devname)
	
	@_fs_should_be_set
	def unfreeze(self):
		return self._fs.unfreeze(self.devname)
	
	def mounted(self):
		res = re.search('%s\s+on\s+(?P<mpoint>.+)\s+type' % self.devname, system(MOUNT_PATH)[0])
		return bool(res)		

	def mount(self, mpoint=None):
		mpoint = mpoint or self.mpoint
		cmd = (MOUNT_PATH, self.devname, mpoint)
		system(cmd, error_text='Cannot mount device %s' % self.devname)
		self.mpoint = mpoint
	
	def umount(self):
		cmd = (UMOUNT_PATH, '-f', self.devname)
		try:
			system(cmd, error_text='Cannot umount device %s' % self.devname)
		except BaseException, e:
			if not 'not mounted' in str(e):
				raise
	
	def snapshot(self, description=None):
		if self._fs:
			system(SYNC_PATH)
			self.freeze()
		snap = self._create_snapshot(description)
		if self._fs:
			self.unfreeze()
		return self._save_snapshot(snap)
	
	def _create_snapshot(self, description):
		return Snapshot(None, description)
	
	def _save_snapshot(self, snapshot):
		return snapshot
	
	def restore(self, snapshot):
		pass


class Snapshot:
	description = None
	id = None
	def __init__(self, id=None, description=None):
		self.id = id
		self.description = description


class EphVolume(Volume):
	snap_pvd = None
	snap_backend = None
	vg = None
	tranzit_vol = None
	
	def __init__(self, devname, mpoint=None, fstype=None, vg=None, tranzit_lv=None, snap_pvd=None, snap_backend=None):
		Volume.__init__(self, devname, mpoint, fstype)
		self.snap_pvd = snap_pvd
		if isinstance(snap_backend, IEphSnapshotBackend):
			self.snap_backend = snap_backend
		elif snap_backend:
			self.snap_backend = ResourceMgr.lookup_snapshot_backend(snap_backend)
		self.vg = vg		
		self.tranzit_vol = Volume(tranzit_lv, '/tmp/sntz' + str(randint(100, 999)), 'ext3') 
	
	def _prepare(self):
		os.makedirs(self.tranzit_vol.mpoint)
		self.tranzit_vol.mkfs()
		self.tranzit_vol.mount()
		
	def _cleanup(self):
		if self.tranzit_vol.mounted():
			self.tranzit_vol.umount()
		os.rmdir(self.tranzit_vol.mpoint)
	
	def _create_snapshot(self, description):
		try:
			self._prepare()
			snapshot = Snapshot(None, description)
			self.snap_pvd.create(snapshot, self, self.tranzit_vol.mpoint)
			return snapshot
		except:
			self._cleanup()
			raise
	
	def _save_snapshot(self, snapshot):
		try:
			self.snap_backend.upload(snapshot, self.tranzit_vol.mpoint)
			return snapshot
		finally:
			self._cleanup()

	def restore(self, snapshot):
		# Lookup snapshot backend
		u = urlparse.urlparse(snapshot.id)
		try:
			snap_backend = ResourceMgr.lookup_snapshot_backend(u.scheme)
		except LookupError, e:
			raise StorageError(e)

		self._prepare()
		try:
			# Download and restore snapshot			
			snap_backend.download(snapshot.id, self.tranzit_vol.mpoint)
			self.snap_pvd.restore(self, self.tranzit_vol.mpoint)
		finally:
			self._cleanup()
			
	def remove(self):
		# TODO:
		pass


class IEphSnapshotBackend:
	scheme = None
	
	def upload(self, snapshot, tranzit_path):
		# Save snapshot from `tranzit_path` to cloud storage and updates it `id`
		pass
	def download(self, id, tranzit_path):
		# Load snapshot from cloud storage to `tranzit_path`
		pass

class IEphSnapshotProvider:
	
	def create(self, snapshot, volume, tranzit_path):
		# Creates snapshot of `volume` under `tranzit_path`
		pass
	
	def restore(self, volume, tranzit_path):
		# Restores snapshot from `tranzit_path`
		pass
	
	
class EphSnapshotProvider(IEphSnapshotProvider):

	MANIFEST_NAME 		= 'manifest.ini'
	SNAPSHOT_LV_NAME 	= 'snap'	
	CHUNK_PREFIX 		= 'data'
	
	chunk_size = None
	'''	Data chunk size in Mb '''
	
	_logger = None
	
	def __init__(self, chunk_size=10):
		self.chunk_size = chunk_size		
		self._logger = logging.getLogger(__name__)
		self._lvm = Lvm2()
	
	def create(self, snapshot, volume, tranzit_path):
		# Create LVM snapshot
		snap_devname = None
		try:
			snap_devname = self._lvm.create_lv_snapshot(volume.devname, self.SNAPSHOT_LV_NAME, extents='100%FREE')
			
			# Copy|gzip|split snapshot into tranzit volume directory
			self._logger.info('Packing volume %s -> %s', volume.devname, tranzit_path) 
			cmd1 = ['dd', 'if=%s' % snap_devname]
			cmd2 = ['gzip']
			cmd3 = ['split', '-a','3', '-d' '-b', '%sM' % self.chunk_size, '-', '%s/%s.gz.' % 
					(tranzit_path, self.CHUNK_PREFIX)]
			p1 = subprocess.Popen(cmd1, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
			p2 = subprocess.Popen(cmd2, stdin=p1.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
			p3 = subprocess.Popen(cmd3, stdin=p2.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
			out, err = p3.communicate()
			if p3.returncode:
				raise StorageError('Error during coping LVM snapshot device (code: %d) <out>: %s <err>: %s' % 
						(p3.returncode, out, err))
		finally:
			# Remove LVM snapshot			
			if snap_devname:
				self._lvm.remove_lv(snap_devname)			
					
		# Make snapshot manifest
		config = Configuration('ini')
		config.add('snapshot/description', snapshot.description, force=True)
		config.add('snapshot/created_at', time.strftime("%Y-%m-%d %H:%M:%S"))
		config.add('snapshot/pack_method', 'gzip') # Not used yet
		for chunk in glob.glob(os.path.join(tranzit_path, self.CHUNK_PREFIX)):
			config.add('chunks/%s' % os.path.basename(chunk), self._md5sum(chunk), force=True)
		
		manifest_path = os.path.join(tranzit_path, self.MANIFEST_NAME)
		config.write(manifest_path)

		snapshot.id = manifest_path
		return snapshot
	
	def restore(self, volume, tranzit_path):
		# Load manifest
		mnf = Configuration('ini')
		mnf.read(os.path.join(tranzit_path, self.MANIFEST))
		
		# Checksum
		for chunk, md5sum_o in mnf.get_dict('chunks').items():
			chunkpath = os.path.join(tranzit_path, chunk)
			md5sum_a = self._md5sum(chunkpath)
			if md5sum_a != md5sum_o:
				raise StorageError(
						'Chunk file %s checksum mismatch. Actial md5sum %s != %s defined in snapshot manifest', 
						chunkpath, md5sum_a, md5sum_o)
		
		# Restore chunks 
		self._logger.info('Unpacking snapshot from %s -> %s', tranzit_path, volume.devname)
		cat = ['cat']
		cat.extend(os.path.join(tranzit_path, chunk) for chunk in mnf.get_dict('chunks'))
		gunzip = ['gunzip']
		dest = open(volume.devname, 'w')
		#Todo: find out where to extract file
		p1 = subprocess.Popen(cat, stdout=subprocess.PIPE)
		p2 = subprocess.Popen(gunzip, stdin=p1.stdout, stdout=dest)
		out, err = p2.communicate()
		dest.close()		
		if p2.returncode:
			raise StorageError('Error during snapshot restoring (code: %d) <out>: %s <err>: %s' % 
					(p2.returncode, out, err))


	def _md5sum(self, file, block_size=4096):
		md5 = hashlib.md5()
		while True:
			data = file.read(block_size)
			if not data:
				break
			md5.update(data)
		return md5.digest()


class EphSnapshotBackend(IEphSnapshotBackend):
	uploader = None
	def __init__(self, scheme, uploader):
		self.scheme = scheme
		self.uploader = uploader
	
	def upload(self, snapshot, tranzit_path):
		# Save snapshot from `tranzit_vol` to cloud storage and updates it `id`
		pass
	
	def download(self, id, tranzit_path):
		# Load snapshot from cloud storage to `tranzit_vol` (it should be mounted)
		pass
	
	
class RaidVolume(Volume):
	pass