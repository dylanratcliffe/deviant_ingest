
serviceName�
�

Restart
on-failure

ExecMainPID	     ��@
8
Path0./org/freedesktop/systemd1/unit/crond_2eservice

	LoadStateloaded

GuessMainPID 

MemoryCurrent	     �BA
m

ExecReload_*]
$
fullCMD/bin/kill -HUP $MAINPID

binary	/bin/kill

args2
-HUP

$MAINPID

FragmentPath* 

NotifyAccessnone

ActiveStateactive

Namecrond.service

ExecCondition2 

SubState	running

Typesimple
x
	ExecStartk*i

args2
-n

$CRONDARGS
*
fullCMD/usr/sbin/crond -n $CRONDARGS

binary/usr/sbin/crond
"
DescriptionCommand Scheduler"�systemd.service
service �**�2return.item._INBOX.YUAfGqdDedSMbQa81pc5fB.arSO7yDu�6return.response._INBOX.YUAfGqdDedSMbQa81pc5fB.6vfJQYy1"��������*���2���:systemd*centos8.localdomain�#
process887centos8.localdomain�,
file/usr/sbin/crondcentos8.localdomain�&
file	/bin/killcentos8.localdomain