
serviceName�
�

	LoadStateloaded

SubStateexited
�
	ExecStart|*z

args2

reboot
4
fullCMD)'/lib/systemd/systemd-update-utmp reboot
,
binary" /lib/systemd/systemd-update-utmp
7
Description(&Update UTMP about System Boot/Shutdown

GuessMainPID 

ActiveStateactive

NotifyAccessnone

RemainAfterExit 

	OOMPolicystop

FragmentPath* 

ExecMainPID	     �@

Restartno
�
ExecStop�*~

args2

shutdown
6
fullCMD+)/lib/systemd/systemd-update-utmp shutdown
,
binary" /lib/systemd/systemd-update-utmp

ExecCondition2 
J
PathB@/org/freedesktop/systemd1/unit/systemd_2dupdate_2dutmp_2eservice

Type	oneshot
%
Namesystemd-update-utmp.service"�systemd.service
service �**�2return.item._INBOX.YUAfGqdDedSMbQa81pc5fB.arSO7yDu�6return.response._INBOX.YUAfGqdDedSMbQa81pc5fB.6vfJQYy1"������C*�Έ�2ꑿ:systemd*ubuntu2004.localdomain�@
file /lib/systemd/systemd-update-utmpubuntu2004.localdomain