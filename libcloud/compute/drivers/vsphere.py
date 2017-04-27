# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
VMware vSphere driver. Uses pyvmomi - https://github.com/vmware/pyvmomi
Code inspired by https://github.com/vmware/pyvmomi-community-samples
"""
try:
    from pyVim import connect
    from pyVmomi import vim
except ImportError:
    raise ImportError('Missing "pyvmomi" dependency. You can install it '
                      'using pip - pip install pyvmomi')

import ssl

from libcloud.common.types import LibcloudError, InvalidCredsError
from libcloud.compute.base import NodeDriver
from libcloud.compute.base import NodeLocation
from libcloud.compute.base import NodeImage
from libcloud.compute.base import Node
from libcloud.compute.types import NodeState, Provider
from libcloud.utils.networking import is_public_subnet


class VSphereNodeDriver(NodeDriver):
    name = "VMware vSphere"
    website = "http://www.vmware.com/products/vsphere/"
    type = Provider.VSPHERE

    NODE_STATE_MAP = {
        "poweredOn": NodeState.RUNNING,
        "poweredOff": NodeState.STOPPED,
        "suspended": NodeState.SUSPENDED,
    }

    def __init__(self, host, username, password, verify_ssl=True):
        context = ssl.create_default_context()
        if not verify_ssl:
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE

        try:
            self.conn = connect.SmartConnect(host=host,
                                             user=username,
                                             pwd=password,
                                             sslContext=context)
        except Exception as e:
            if isinstance(e, vim.fault.InvalidLogin):
                raise InvalidCredsError("username/password not acepted")
            raise LibcloudError(str(e), driver=self)

    @property
    def _root(self):
        return self.conn.RetrieveContent().rootFolder

    @property
    def _datacenters(self):
        return [dc for dc in self._root.childEntity
                if isinstance(dc, vim.Datacenter)]

    @property
    def _vms(self):
        vms = []
        nodes = (node for datacenter in self._datacenters
                 for node in datacenter.vmFolder.childEntity)

        def vms_in(folder):
            """ recursively extract the vms from a folder """
            for node in folder.childEntity:
                if isinstance(node, vim.Folder):
                    for vm in vms_in(node):
                        yield vm
                elif isinstance(node, vim.VirtualMachine):
                    yield node

        for node in nodes:
            if isinstance(node, vim.Folder):
                vms.extend(vms_in(node))
            elif isinstance(node, vim.VirtualMachine):
                vms.append(node)

        return vms

    def _get_obj_by_name(self, vimtype, name):
        if not isinstance(vimtype, list):
            vimtype = [vimtype]

        content = self.conn.RetrieveContent()
        container = content.viewManager.CreateContainerView(
            content.rootFolder,
            vimtype,
            True)

        return next((i for i in container.view if i.name == name), None)

    def _get_obj_by_uuid(self, uuid):
        content = self.conn.RetrieveContent()
        return content.searchIndex.FindByUuid(None, uuid, True, True)

    def _get_datacenter(self, obj):
        datacenter = obj.parent
        while not isinstance(datacenter, vim.Datacenter):
            datacenter = datacenter.parent
        return datacenter

    def _to_node(self, vm):
        config = vm.summary.config
        datacenter = self._get_datacenter(vm)

        extra = {
            "path": config.vmPathName,
            "operating_system": config.guestFullName,
            "os_type": "windows" if "Microsoft" in config.guestFullName \
                       else "unix",
            "memory_MB": config.memorySizeMB,
            "cpus": config.numCpu,
            "overallStatus": str(vm.summary.overallStatus),
            "datacenter": datacenter,
        }

        kwargs = {
            "id": config.instanceUuid,
            "name": config.name,
            "state": self.NODE_STATE_MAP.get(vm.summary.runtime.powerState,
                                             NodeState.UNKNOWN),
            "public_ips": [],
            "private_ips": [],
            "driver": self,
            "extra": extra,
        }

        for nic in vm.guest.net:
            for addr in nic.ipAddress:
                try:
                    if is_public_subnet(addr):
                        kwargs['public_ips'].append(addr)
                    else:
                        kwargs['private_ips'].append(addr)
                except OSError:  # inet_aton error for ipv6 addrs
                    pass

        node = Node(**kwargs)
        node._uuid = kwargs['id']
        return node

    def list_locations(self):
        """ Return the clusters available on this vSphere instance """
        return [NodeLocation(id="/".join(dc.name, cl.name),
                             name=cl.name,
                             country=None,
                             driver=self)
                for dc in self._datacenters
                for cl in dc.hostFolder.childEntity]

    def list_nodes(self):
        """ Return vms not marked as templates """
        return [self._to_node(n) for n in self._vms
                if n.summary.config.vmPathName.endswith(".vmx")]

    def list_images(self):
        """ Return all the vms set up as templates """
        imgs = [n for n in self._vms 
                if n.summary.config.vmPathName.endswith(".vmtx")]
        return [NodeImage(id=img.summary.config.instanceUuid,
                          name=img.name,
                          driver=self)
                for img in imgs]

    def reboot_node(self, node):
        n = self._get_obj_by_uuid(node.id)
        if node.state == 'stopped':
            n.PowerOn()
        else:
            n.RebootGuest()

    def destroy_node(self, node):
        n = self._get_obj_by_uuid(node.id)
        node = self._to_node(n)
        if node.state != 'stopped':
            task = n.PowerOff()
            while not task.info.completeTime:
                pass
        n.Destroy()

    def create_node(self, name, location, clone_from):
        template = self._get_obj_by_uuid(clone_from.id)
        datacenter = self._get_datacenter(template)
        cluster = self._get_obj_by_name(vim.ClusterComputeResource,
                                        location.name)
        pool = cluster.resourcePool

        relospec = vim.vm.RelocateSpec()
        relospec.datastore = template.datastore[0]
        relospec.pool = pool

        clonespec = vim.vm.CloneSpec()
        clonespec.location = relospec
        clonespec.powerOn = True

        task = template.Clone(folder=datacenter.vmFolder,
                              name=name,
                              spec=clonespec)
        return task

    def ex_stop_node(self, node):
        n = self._get_obj_by_uuid(node.id)
        n.PowerOff()

    def ex_start_node(self, node):
        n = self._get_obj_by_uuid(node.id)
        n.PowerOn()
