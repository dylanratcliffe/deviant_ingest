
serviceName�
�

Type	oneshot
C
Path;9/org/freedesktop/systemd1/unit/lxd_2dagent_2d9p_2eservice

	OOMPolicystop

Restartno

ActiveState
inactive

MemoryCurrent	      �C

GuessMainPID 

	LoadStateloaded

FragmentPath* 
'
DescriptionLXD - agent - 9p mount

ExecCondition2 
�
	ExecStart�*�

binary
/bin/mount
U
argsM2K
-t
9p
config
/run/lxd_config/9p
-o
access=0,trans=virtio
P
fullCMDEC/bin/mount -t 9p config /run/lxd_config/9p -o access=0,trans=virtio

Namelxd-agent-9p.service

SubStatedead

NotifyAccessnone
�
ExecStartPreq*o

binary
/bin/chmod
&
args2
0700
/run/lxd_config/
-
fullCMD" /bin/chmod 0700 /run/lxd_config/

RemainAfterExit "�systemd.service
service �**�2return.item._INBOX.YUAfGqdDedSMbQa81pc5fB.arSO7yDu�6return.response._INBOX.YUAfGqdDedSMbQa81pc5fB.6vfJQYy1"�����ūD*�Έ�2ꑿ:systemd*ubuntu2004.localdomain�.
file/sbin/modprobeubuntu2004.localdomain�*
file
/bin/mkdirubuntu2004.localdomain�*
file
/bin/chmodubuntu2004.localdomain�*
file
/bin/mountubuntu2004.localdomain