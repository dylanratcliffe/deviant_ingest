
serviceName�
�

Typesimple

Namesssd-kcm.service

FragmentPath* 
,
DescriptionSSSD Kerberos Cache Manager

MemoryCurrent	      �C
�
	ExecStart�*�
&
binary/usr/libexec/sssd/sssd_kcm
9
args12/
--uid
0
--gid
0
${DEBUG_LOGGER}
G
fullCMD<:/usr/libexec/sssd/sssd_kcm --uid 0 --gid 0 ${DEBUG_LOGGER}

ExecCondition2 
�
ExecStartPrev*t

binary/usr/sbin/sssd
#
args2
--genconf-section=kcm
1
fullCMD&$/usr/sbin/sssd --genconf-section=kcm

SubStatedead

	LoadStateloaded

GuessMainPID 

Restartno

NotifyAccessnone
=
Path53/org/freedesktop/systemd1/unit/sssd_2dkcm_2eservice

ActiveState
inactive"�systemd.service
service �**�2return.item._INBOX.YUAfGqdDedSMbQa81pc5fB.arSO7yDu�6return.response._INBOX.YUAfGqdDedSMbQa81pc5fB.6vfJQYy1"�����׉*���2���:systemd*centos8.localdomain�+
file/usr/sbin/sssdcentos8.localdomain�7
file/usr/libexec/sssd/sssd_kcmcentos8.localdomain