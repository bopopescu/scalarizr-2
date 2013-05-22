__author__ = 'Nick Demyanchuk'

import os
import time
import sys
import uuid
import logging
import datetime

from scalarizr import storage2
from scalarizr.node import __node__
from scalarizr.storage2.volumes import base
from scalarizr.storage2.util import gce as gce_util

LOG = logging.getLogger(__name__)

class GcePersistentVolume(base.Volume):
    '''
    def _get_device_name(self):
            return 'google-%s' % (self.alias or self.name)
    '''


    def __init__(self, name=None, link=None, size=None, zone=None,
                                            last_attached_to=None, **kwargs):
        name = name or 'scalr-disk-%s' % uuid.uuid4().hex[:8]
        super(GcePersistentVolume, self).__init__(name=name, link=link,
                                                                                          size=size, zone=zone,
                                                                                          last_attached_to=last_attached_to,
                                                                                          **kwargs)


    def _ensure(self):

        garbage_can = []
        zone = os.path.basename(__node__['gce']['zone'])
        project_id = __node__['gce']['project_id']
        server_name = __node__['server_id']

        try:
            connection = __node__['gce']['compute_connection']
        except:
            """ No connection, implicit check """
            try:
                self._check_attr('name')
            except:
                raise storage2.StorageError('Disk is not created yet, and GCE connection'
                                            ' is unavailable')
            device = gce_util.devicename_to_device(self.name)
            if not device:
                raise storage2.StorageError("Disk is not attached and GCE connection is unavailable")

            self.device = device
        else:

            try:
                # TODO(spike) raise VolumeNotExistsError when link passed disk not exists
                create = False
                if not self.link:
                    # Disk does not exist, create it first
                    create_request_body = dict(name=self.name, sizeGb=self.size)
                    if self.snap:
                        self.snap = storage2.snapshot(self.snap)
                        create_request_body['sourceSnapshot'] = self.snap.link
                    create = True
                else:
                    self._check_attr('zone')
                    if self.zone != zone:
                        # Volume is in different zone, snapshot it,
                        # create new volume from this snapshot, then attach
                        temp_snap = self.snapshot('volume')
                        garbage_can.append(temp_snap)
                        new_name = self.name + zone
                        create_request_body = dict(name=new_name,
                                                   sizeGb=self.size,
                                                   sourceSnapshot=temp_snap.link)
                        create = True

                attach = False
                if create:
                    disk_name = create_request_body['name']
                    LOG.debug('Creating new GCE disk %s' % disk_name)
                    op = connection.disks().insert(project=project_id,
                                                   zone=zone,
                                                   body=create_request_body).execute()
                    gce_util.wait_for_operation(connection, project_id, op['name'], zone)
                    disk_dict = connection.disks().get(disk=disk_name,
                                                       project=project_id,
                                                       zone=zone).execute()
                    self.id = disk_dict['id']
                    self.link = disk_dict['selfLink']
                    self.zone = zone
                    self.name = disk_name
                    attach = True

                else:
                    if self.last_attached_to and self.last_attached_to != server_name:
                        LOG.debug("Making sure that disk %s detached from previous attachment place." % self.name)
                        gce_util.ensure_disk_detached(connection, project_id, zone, self.last_attached_to, self.link)

                    attachment_inf = self._attachment_info(connection)
                    if attachment_inf:
                        disk_devicename = attachment_inf['deviceName']
                    else:
                        attach = True

                if attach:
                    LOG.debug('Attaching disk %s to current instance' % self.name)
                    op = connection.instances().attachDisk(
                                            instance=server_name,
                                            project=project_id,
                                            zone=zone,
                                            body=dict(
                                                            deviceName=self.name,
                                                            source=self.link,
                                                            mode="READ_WRITE",
                                                            type="PERSISTENT"
                                            )).execute()
                    gce_util.wait_for_operation(connection, project_id, op['name'], zone=zone)
                    disk_devicename = self.name

                device = gce_util.devicename_to_device(disk_devicename)
                if not device:
                    raise storage2.StorageError("Disk should be attached, but corresponding"
                                                                            " device not found in system")
                self.device = device
                self.last_attached_to = server_name
                self.snap = None

            finally:
                # Perform cleanup
                for garbage in garbage_can:
                    try:
                        garbage.destroy(force=True)
                    except:
                        pass


    def _attachment_info(self, con):
        zone = os.path.basename(__node__['gce']['zone'])
        project_id = __node__['gce']['project_id']
        server_name = __node__['server_id']

        return gce_util.attachment_info(con, project_id, zone, server_name, self.link)


    def _detach(self, force, **kwds):
        connection = __node__['gce']['compute_connection']
        attachment_inf = self._attachment_info(connection)
        if attachment_inf:
            zone = os.path.basename(__node__['gce']['zone'])
            project_id = __node__['gce']['project_id']
            server_name = __node__['server_id']

            def try_detach():
                op = connection.instances().detachDisk(instance=server_name,
                                                                    project=project_id,
                                                                    zone=zone,
                                                                    deviceName=attachment_inf['deviceName']).execute()

                gce_util.wait_for_operation(connection, project_id, op['name'], zone=zone)

            for _time in range(3):
                try:
                    try_detach()
                except:
                    e = sys.exc_info()[1]
                    LOG.debug('Detach disk attempt failed: %s' % e)
                    if _time == 2:
                        raise storage2.StorageError('Can not detach disk: %s' % e)
                    time.sleep(1)
                    LOG.debug('Trying to detach disk again.')


    def _destroy(self, force, **kwds):
        self._check_attr('link')
        self._check_attr('name')

        connection = __node__['gce']['compute_connection']
        project_id = __node__['gce']['project_id']
        zone = os.path.basename(__node__['gce']['zone'])
        try:
            op = connection.disks().delete(project=project_id,
                                                                       zone=zone,
                                                                       disk=self.name).execute()
            gce_util.wait_for_operation(connection, project_id, op['name'], zone=zone)
        except:
            e = sys.exc_info()[1]
            raise storage2.StorageError("Disk destruction failed: %s" % e)


    def _snapshot(self, description, tags, **kwds):
        """
        :param nowait: if True - do not wait for snapshot to complete, just create and return
        """
        connection = __node__['gce']['compute_connection']
        project_id = __node__['gce']['project_id']
        nowait = kwds.get('nowait', True)

        now_raw = datetime.datetime.utcnow()
        now_str = now_raw.strftime('%d-%b-%Y-%H-%M-%S-%f')
        snap_name = ('%s-snap-%s' % (self.name, now_str)).lower()

        operation = connection.snapshots().insert(project=project_id,
                                        body=dict(
                                                name=snap_name,
                                                # Doesnt work without kind (3.14.2013)
                                                kind="compute#snapshot",
                                                description=description,
                                                sourceDisk=self.link,
                                        )).execute()
        try:
            # Wait until operation at least started
            gce_util.wait_for_operation(connection, project_id, operation['name'],
                                                            status_to_wait=("DONE", "RUNNING"))
            # If nowait=false, wait until operation is totally complete
            snapshot_info = connection.snapshots().get(project=project_id,
                                                                            snapshot=snap_name,
                                                                            fields='id,name,diskSizeGb,selfLink').execute()

            snapshot = GcePersistentSnapshot(id=snapshot_info['id'],
                                                                             name=snapshot_info['name'],
                                                                             size=snapshot_info['diskSizeGb'],
                                                                             link=snapshot_info['selfLink'])
            if not nowait:
                while True:
                    status = snapshot.status()
                    if status == snapshot.COMPLETED:
                        break
                    elif status == snapshot.FAILED:
                        raise Exception('Snapshot status is "Failed"')
            return snapshot
        except:
            e = sys.exc_info()[1]
            raise storage2.StorageError('Google disk snapshot creation '
                            'failed. Error: %s' % e)




class GcePersistentSnapshot(base.Snapshot):

    def __init__(self, name, **kwds):
        super(GcePersistentSnapshot, self).__init__(name=name, **kwds)


    def _destroy(self):
        try:
            connection = __node__['gce']['compute_connection']
            project_id = __node__['gce']['project_id']

            op = connection.snapshots().delete(project=project_id,
                                                                            snapshot=self.name).execute()

            gce_util.wait_for_operation(connection, project_id,
                                                                       op['name'])
        except:
            e = sys.exc_info()[1]
            raise storage2.StorageError('Failed to delete google disk snapshot.'
                                                                    ' Error: %s' % e)

    def _status(self):
        self._check_attr("name")
        connection = __node__['gce']['compute_connection']
        project_id = __node__['gce']['project_id']
        snapshot = connection.snapshots().get(project=project_id, snapshot=self.name,
                                                                                  fields='status').execute()
        status = snapshot['status']
        status_map = dict(CREATING=self.IN_PROGRESS, UPLOADING=self.IN_PROGRESS,
                                           READY=self.COMPLETED, ERROR=self.FAILED, FAILED=self.FAILED)
        return status_map.get(status, self.UNKNOWN)




storage2.volume_types['gce_persistent'] = GcePersistentVolume
storage2.snapshot_types['gce_persistent'] = GcePersistentSnapshot
