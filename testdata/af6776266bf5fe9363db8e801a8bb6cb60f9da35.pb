
serviceName�
�
(
Name systemd-tmpfiles-setup.service

Type	oneshot

ExecMainPID	     `@
-
SuccessExitStatus2
	     @P@
	     @R@

Restartno

ExecCondition2 
M
PathEC/org/freedesktop/systemd1/unit/systemd_2dtmpfiles_2dsetup_2eservice
�
	ExecStart�*�
U
fullCMDJH/usr/bin/systemd-tmpfiles --create --remove --boot --exclude-prefix=/dev
%
binary/usr/bin/systemd-tmpfiles
E
args=2;

--create

--remove
--boot
--exclude-prefix=/dev

	LoadStateloaded

ActiveStateactive
6
Description'%Create Volatile Files and Directories

SubStateexited

RemainAfterExit 

GuessMainPID 

	OOMPolicystop

NotifyAccessnone

FragmentPath* "�systemd.service
service �**�2return.item._INBOX.YUAfGqdDedSMbQa81pc5fB.arSO7yDu�6return.response._INBOX.YUAfGqdDedSMbQa81pc5fB.6vfJQYy1"�������J*�Έ�2ꑿ:systemd*ubuntu2004.localdomain�9
file/usr/bin/systemd-tmpfilesubuntu2004.localdomain