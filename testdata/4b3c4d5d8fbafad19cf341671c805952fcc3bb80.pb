
serviceName�
�

NotifyAccessnone

Restartno

FragmentPath* 

GuessMainPID 
^
DescriptionOMMonitoring of LVM2 mirrors, snapshots etc. using dmeventd or progress polling
�
	ExecStartx*v
(
args 2

vgchange
	--monitor
y
/
fullCMD$"/usr/sbin/lvm vgchange --monitor y

binary/usr/sbin/lvm
�
ExecStopx*v
(
args 2

vgchange
	--monitor
n
/
fullCMD$"/usr/sbin/lvm vgchange --monitor n

binary/usr/sbin/lvm
A
Path97/org/freedesktop/systemd1/unit/lvm2_2dmonitor_2eservice

	LoadStateloaded

ActiveStateactive

ExecMainPID	     ��@

SubStateexited

Type	oneshot

RemainAfterExit 

Namelvm2-monitor.service

ExecCondition2 "�systemd.service
service �**�2return.item._INBOX.YUAfGqdDedSMbQa81pc5fB.arSO7yDu�6return.response._INBOX.YUAfGqdDedSMbQa81pc5fB.6vfJQYy1"��������*���2���:systemd*centos8.localdomain�*
file/usr/sbin/lvmcentos8.localdomain