
serviceName�
�

	OOMPolicystop
)
RestartPreventExitStatus2
	     �o@

Restart
on-failure

MemoryCurrent	     �OA
,
DescriptionOpenBSD Secure Shell server

Namessh.service

ExecMainPID	     �@

ActiveStateactive
6
Path.,/org/freedesktop/systemd1/unit/ssh_2eservice
`
ExecStartPreP*N

args2
-t

fullCMD/usr/sbin/sshd -t

binary/usr/sbin/sshd

	LoadStateloaded

SubState	running

ExecCondition2 
v
	ExecStarti*g

binary/usr/sbin/sshd

args2
-D

$SSHD_OPTS
)
fullCMD/usr/sbin/sshd -D $SSHD_OPTS

FragmentPath* 

GuessMainPID 
m

ExecReload_*]

binary	/bin/kill

args2
-HUP

$MAINPID
$
fullCMD/bin/kill -HUP $MAINPID

Typenotify

NotifyAccessmain"�systemd.service
service �**�2return.item._INBOX.YUAfGqdDedSMbQa81pc5fB.arSO7yDu�6return.response._INBOX.YUAfGqdDedSMbQa81pc5fB.6vfJQYy1"����ȑ�C*�Έ�2ꑿ:systemd*ubuntu2004.localdomain�&
process701ubuntu2004.localdomain�.
file/usr/sbin/sshdubuntu2004.localdomain�)
file	/bin/killubuntu2004.localdomain