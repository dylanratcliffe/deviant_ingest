
serviceName�
�

	OOMPolicystop

Type	oneshot
�
	ExecStartx*v
*
binary /usr/lib/apt/apt.systemd.daily

args2

update
2
fullCMD'%/usr/lib/apt/apt.systemd.daily update

GuessMainPID 

Restartno

	LoadStateloaded

ExecCondition2 

ExecMainPID	     �@

NotifyAccessnone
>
Path64/org/freedesktop/systemd1/unit/apt_2ddaily_2eservice

SubStatedead

Nameapt-daily.service

ActiveState
inactive
�
ExecStartPret*r
#
binary/usr/lib/apt/apt-helper

args2
wait-online
0
fullCMD%#/usr/lib/apt/apt-helper wait-online

MemoryCurrent	      �C
.
DescriptionDaily apt download activities

FragmentPath* "�systemd.service
service �**�2return.item._INBOX.YUAfGqdDedSMbQa81pc5fB.arSO7yDu�6return.response._INBOX.YUAfGqdDedSMbQa81pc5fB.6vfJQYy1"�����ʤJ*�Έ�2ꑿ:systemd*ubuntu2004.localdomain�7
file/usr/lib/apt/apt-helperubuntu2004.localdomain�>
file/usr/lib/apt/apt.systemd.dailyubuntu2004.localdomain