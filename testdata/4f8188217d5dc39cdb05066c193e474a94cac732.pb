
serviceName�
�
-
SuccessExitStatus2
	     @P@
	     @R@

ActiveStateactive

GuessMainPID 
�
	ExecStart�*�
%
binary/usr/bin/systemd-tmpfiles
E
args=2;

--create

--remove
--boot
--exclude-prefix=/dev
U
fullCMDJH/usr/bin/systemd-tmpfiles --create --remove --boot --exclude-prefix=/dev

ExecCondition2 
6
Description'%Create Volatile Files and Directories

SubStateexited

FragmentPath* 

ExecMainPID	     0�@

NotifyAccessnone
(
Name systemd-tmpfiles-setup.service

RemainAfterExit 

Type	oneshot
M
PathEC/org/freedesktop/systemd1/unit/systemd_2dtmpfiles_2dsetup_2eservice

Restartno

	LoadStateloaded"�systemd.service
service �**�2return.item._INBOX.YUAfGqdDedSMbQa81pc5fB.arSO7yDu�6return.response._INBOX.YUAfGqdDedSMbQa81pc5fB.6vfJQYy1"�����숊*���2���:systemd*centos8.localdomain�6
file/usr/bin/systemd-tmpfilescentos8.localdomain