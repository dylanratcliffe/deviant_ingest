
serviceName�
�

RemainAfterExit 
�
	ExecStart�*�
0
args(2&
-f
/etc/sysconfig/nftables.conf
6
fullCMD+)/sbin/nft -f /etc/sysconfig/nftables.conf

binary	/sbin/nft

	LoadStateloaded

ActiveState
inactive

GuessMainPID 
!
DescriptionNetfilter Tables

Namenftables.service

SubStatedead

Restartno

Type	oneshot
k
ExecStop_*]

args2
flush
	ruleset
$
fullCMD/sbin/nft flush ruleset

binary	/sbin/nft

NotifyAccessnone
�

ExecReload�*�
M
fullCMDB@/sbin/nft flush ruleset; include "/etc/sysconfig/nftables.conf";

binary	/sbin/nft
D
args<2:
86flush ruleset; include "/etc/sysconfig/nftables.conf";
;
Path31/org/freedesktop/systemd1/unit/nftables_2eservice

ExecCondition2 

MemoryCurrent	      �C

FragmentPath* "�systemd.service
service �**�2return.item._INBOX.YUAfGqdDedSMbQa81pc5fB.arSO7yDu�6return.response._INBOX.YUAfGqdDedSMbQa81pc5fB.6vfJQYy1"������ي*���2���:systemd*centos8.localdomain�&
file	/sbin/nftcentos8.localdomain