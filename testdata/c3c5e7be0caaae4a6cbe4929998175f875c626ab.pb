
serviceName�
�
&
DescriptionLoad/Save Random Seed

NotifyAccessnone

	OOMPolicystop

ExecCondition2 

Type	oneshot

GuessMainPID 

Restartno
%
Namesystemd-random-seed.service

ActiveStateactive

FragmentPath* 

ExecMainPID	      v@

SubStateexited

	LoadStateloaded
�
	ExecStartx*v

args
2
load
2
fullCMD'%/lib/systemd/systemd-random-seed load
,
binary" /lib/systemd/systemd-random-seed
�
ExecStopx*v
,
binary" /lib/systemd/systemd-random-seed

args
2
save
2
fullCMD'%/lib/systemd/systemd-random-seed save
J
PathB@/org/freedesktop/systemd1/unit/systemd_2drandom_2dseed_2eservice

RemainAfterExit "�systemd.service
service �**�2return.item._INBOX.YUAfGqdDedSMbQa81pc5fB.arSO7yDu�6return.response._INBOX.YUAfGqdDedSMbQa81pc5fB.6vfJQYy1"�����K*�Έ�2ꑿ:systemd*ubuntu2004.localdomain�@
file /lib/systemd/systemd-random-seedubuntu2004.localdomain