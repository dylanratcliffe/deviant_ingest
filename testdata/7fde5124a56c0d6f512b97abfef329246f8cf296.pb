
serviceName�
�

ExecMainPID	     Ѕ@

Nameatd.service
-
DescriptionDeferred execution scheduler

Typesimple

GuessMainPID 

ActiveStateactive

MemoryCurrent	     �#A

NotifyAccessnone
�
ExecStartPre�*�
e
fullCMDZX/usr/bin/find /var/spool/cron/atjobs -type f -name =* -not -newercc /run/systemd -delete

binary/usr/bin/find
p
argsh2f
/var/spool/cron/atjobs
-type
f
-name
=*
-not

-newercc
/run/systemd
	-delete

	OOMPolicystop

FragmentPath* 

ExecCondition2 

	LoadStateloaded
6
Path.,/org/freedesktop/systemd1/unit/atd_2eservice

Restart
on-failure
[
	ExecStartN*L

args2
-f

fullCMD/usr/sbin/atd -f

binary/usr/sbin/atd

SubState	running"�systemd.service
service �**�2return.item._INBOX.YUAfGqdDedSMbQa81pc5fB.arSO7yDu�6return.response._INBOX.YUAfGqdDedSMbQa81pc5fB.6vfJQYy1"�����ГK*�Έ�2ꑿ:systemd*ubuntu2004.localdomain�&
process698ubuntu2004.localdomain�-
file/usr/sbin/atdubuntu2004.localdomain�-
file/usr/bin/findubuntu2004.localdomain