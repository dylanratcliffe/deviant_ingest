
serviceName�
�

FragmentPath* 

Namenetworking.service

SubStateexited

RemainAfterExit 

Restartno
)
DescriptionRaise network interfaces

ExecMainPID	     8�@

Type	oneshot

	LoadStateloaded

	OOMPolicystop
=
Path53/org/freedesktop/systemd1/unit/networking_2eservice

GuessMainPID 

NotifyAccessnone

ActiveStateactive

ExecCondition2 
~
	ExecStartq*o
&
args2
-a
--read-environment
-
fullCMD" /sbin/ifup -a --read-environment

binary
/sbin/ifup
�
ExecStop�*�

binary/sbin/ifdown
6
args.2,
-a
--read-environment
--exclude=lo
<
fullCMD1//sbin/ifdown -a --read-environment --exclude=lo"�systemd.service
service �**�2return.item._INBOX.YUAfGqdDedSMbQa81pc5fB.arSO7yDu�6return.response._INBOX.YUAfGqdDedSMbQa81pc5fB.6vfJQYy1"�������L*�Έ�2ꑿ:systemd*ubuntu2004.localdomain�*
file
/sbin/ifupubuntu2004.localdomain�,
file/sbin/ifdownubuntu2004.localdomain