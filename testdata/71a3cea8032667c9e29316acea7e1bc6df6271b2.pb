
serviceName�
�

ActiveStateactive

SubState	running

MemoryCurrent	     �iA

GuessMainPID 
t

ExecReloadf*d

binary/sbin/multipathd

args2
reconfigure
)
fullCMD/sbin/multipathd reconfigure
j
	ExecStart]*[

args2
-d
-s
#
fullCMD/sbin/multipathd -d -s

binary/sbin/multipathd

ExecMainPID	      ~@

Restartno
�
ExecStartPre�*�

binary/sbin/modprobe
O
argsG2E
-a
scsi_dh_alua
scsi_dh_emc
scsi_dh_rdac
dm-multipath
Q
fullCMDFD/sbin/modprobe -a scsi_dh_alua scsi_dh_emc scsi_dh_rdac dm-multipath
:
Description+)Device-Mapper Multipath Device Controller

	LoadStateloaded

FragmentPath* 

NotifyAccessmain

	OOMPolicystop

Typenotify

Namemultipathd.service
=
Path53/org/freedesktop/systemd1/unit/multipathd_2eservice

ExecCondition2 "�systemd.service
service �**�2return.item._INBOX.YUAfGqdDedSMbQa81pc5fB.arSO7yDu�6return.response._INBOX.YUAfGqdDedSMbQa81pc5fB.6vfJQYy1"�������O*�Έ�2ꑿ:systemd*ubuntu2004.localdomain�&
process482ubuntu2004.localdomain�.
file/sbin/modprobeubuntu2004.localdomain�0
file/sbin/multipathdubuntu2004.localdomain