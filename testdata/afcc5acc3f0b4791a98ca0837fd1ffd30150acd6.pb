
serviceName�
�

Restartno

	OOMPolicystop

ActiveStateactive
!
Namevboxadd-service.service
�
	ExecStart�*�
?
binary53/opt/VBoxGuestAdditions-6.1.26/init/vboxadd-service

args2	
start
F
fullCMD;9/opt/VBoxGuestAdditions-6.1.26/init/vboxadd-service start

Type	forking

MemoryCurrent	     �<A

FragmentPath* 
�
ExecStop�*�
?
binary53/opt/VBoxGuestAdditions-6.1.26/init/vboxadd-service

args
2
stop
E
fullCMD:8/opt/VBoxGuestAdditions-6.1.26/init/vboxadd-service stop

	LoadStateloaded

SubState	running

ExecCondition2 
(
Descriptionvboxadd-service.service

RemainAfterExit 
D
Path<:/org/freedesktop/systemd1/unit/vboxadd_2dservice_2eservice

NotifyAccessnone"�systemd.service
service �**�2return.item._INBOX.YUAfGqdDedSMbQa81pc5fB.arSO7yDu�6return.response._INBOX.YUAfGqdDedSMbQa81pc5fB.6vfJQYy1"�����×D*�Έ�2ꑿ:systemd*ubuntu2004.localdomain�S
file3/opt/VBoxGuestAdditions-6.1.26/init/vboxadd-serviceubuntu2004.localdomain