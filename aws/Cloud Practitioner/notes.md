#### YouTube: https://www.youtube.com/watch?v=SOTamWNgDKc


## What is Cloud Computing
- Use a network of remote servers on the internet to store, manage, and process data 
- On-Prem: You own servers, you hire IT people, you rent the real-state, and take all the risk
- Cloud Providers tend to be the opposite 
### Evolution
In the beginning, it stareted with Dedicated servers, a physical machine dedicated to a single business unit. This is expensive, has hihgh maintenance, but high security. Next was a Virtual Private Server (VPS) which was one physical machine dedicated to a single business. The physical machine is virtuliased into sub-machines, and it can run multiple apps. This has better utilisation and isolation of resrouces. Next came shared hosting, which was one physical machine shared by hundreds of machines. This was cheap, but only because it relised on most tennants under utilising their resources. It had limited functinoliaty and poor isolation. Finally, we have Cloud Hosting, which is multiple physical machines, its distributed computing. Flexible, scalable, secure, cost effective. 

##### Dedicated
Cons: You have to guess your capacity, you'll overpay for an underutilised server, you can't vertically scale, your are limited by your host operating system. Multiple apps can result in conflics in resource sharing.
##### VMs
Cons: You still pay for an underutilised Virtual Machine, you're limited by your Guest operating system. Multiple apps on a single Virtual machine can result in conflicts. Hypervisor, VM and each Guest OS operating system take up extra space
##### Containers 
Cons: Still get wasted space in each container. The containers still share the same underlying OS, and are more efficient than multiple VMS, multiple apps can run side by side without being limited to the same OS requiremts and will not sause conflicts during resource sharing
##### Functions 
You upload a piece of code, choose the amutt of memory and duration. Very cost effective, only pay for the time code is running. However this has a side effect of Cold Starts


Chaining together of cloud services creates a cloud architecture
### Common Cloud Services
1) Compute -> Virtual computer that can run applications, programs, code
2) Networking -> Virtual netowrk defined internet connectinos or network isolations between services or outbound to the internet 
3) Storage -> Virtual hard-drive that 
4) Databases -> 
IUt;s basically Infratructure as a service. There are tonnes more services. 