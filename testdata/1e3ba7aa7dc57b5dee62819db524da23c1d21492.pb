
serviceName�
�

NotifyAccessnone

ExecCondition2 

ActiveState
inactive
3
Description$"Secure Boot updates for DB and DBX

MemoryCurrent	      �C

Restartno

	LoadStateloaded

GuessMainPID 

SubStatedead
B
Path:8/org/freedesktop/systemd1/unit/secureboot_2ddb_2eservice

FragmentPath* 
�
ExecStartPre�*�

binary/usr/bin/chattr
V
argsN2L
-i
DB/sys/firmware/efi/efivars/dbx-d719b2cb-3d3a-4596-a3bc-dad00e67656f
b
fullCMDWU/usr/bin/chattr -i /sys/firmware/efi/efivars/dbx-d719b2cb-3d3a-4596-a3bc-dad00e67656f

Namesecureboot-db.service

	OOMPolicystop

Type	oneshot
�
	ExecStart�*�

binary/usr/bin/sbkeysync
`
argsX2V
--no-default-keystores

--keystore
/usr/share/secureboot/updates
	--verbose
i
fullCMD^\/usr/bin/sbkeysync --no-default-keystores --keystore /usr/share/secureboot/updates --verbose"�systemd.service
service �**�2return.item._INBOX.YUAfGqdDedSMbQa81pc5fB.arSO7yDu�6return.response._INBOX.YUAfGqdDedSMbQa81pc5fB.6vfJQYy1"�������L*�Έ�2ꑿ:systemd*ubuntu2004.localdomain�/
file/usr/bin/chattrubuntu2004.localdomain�2
file/usr/bin/sbkeysyncubuntu2004.localdomain