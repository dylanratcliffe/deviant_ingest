
serviceName�
�

ExecCondition2 

	OOMPolicystop

	LoadStateloaded

Type	oneshot

ExecMainPID	     �v@
3
Description$"Create Static Device Nodes in /dev

SubStateexited

FragmentPath* 

NotifyAccessnone

RemainAfterExit 

Restartno
-
SuccessExitStatus2
	     @P@
	     @R@
,
Name$"systemd-tmpfiles-setup-dev.service

ActiveStateactive
�
	ExecStart�*�
%
binary/usr/bin/systemd-tmpfiles
1
args)2'
--prefix=/dev

--create
--boot
D
fullCMD97/usr/bin/systemd-tmpfiles --prefix=/dev --create --boot

GuessMainPID 
S
PathKI/org/freedesktop/systemd1/unit/systemd_2dtmpfiles_2dsetup_2ddev_2eservice"�systemd.service
service �**�2return.item._INBOX.YUAfGqdDedSMbQa81pc5fB.arSO7yDu�6return.response._INBOX.YUAfGqdDedSMbQa81pc5fB.6vfJQYy1"������D*�Έ�2ꑿ:systemd*ubuntu2004.localdomain�9
file/usr/bin/systemd-tmpfilesubuntu2004.localdomain