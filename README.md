<p align="center">
  <a href="https://rclone.org/">
    <img width="20%" alt="PySyncNet" src="https://private-user-images.githubusercontent.com/4536448/334130537-c00669bb-5df1-4294-a047-d4194b790160.png?jwt=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJnaXRodWIuY29tIiwiYXVkIjoicmF3LmdpdGh1YnVzZXJjb250ZW50LmNvbSIsImtleSI6ImtleTUiLCJleHAiOjE3MTY4MTkyNjYsIm5iZiI6MTcxNjgxODk2NiwicGF0aCI6Ii80NTM2NDQ4LzMzNDEzMDUzNy1jMDA2NjliYi01ZGYxLTQyOTQtYTA0Ny1kNDE5NGI3OTAxNjAucG5nP1gtQW16LUFsZ29yaXRobT1BV1M0LUhNQUMtU0hBMjU2JlgtQW16LUNyZWRlbnRpYWw9QUtJQVZDT0RZTFNBNTNQUUs0WkElMkYyMDI0MDUyNyUyRnVzLWVhc3QtMSUyRnMzJTJGYXdzNF9yZXF1ZXN0JlgtQW16LURhdGU9MjAyNDA1MjdUMTQwOTI2WiZYLUFtei1FeHBpcmVzPTMwMCZYLUFtei1TaWduYXR1cmU9NDk4NmNjNDljMWE4YjBiOTBmMWM5NGRjNWM5NTJkZDdkN2U3NTQ3NmNlOTNiMTI1YmVmOGRmOTM2ZmMxNWM4NyZYLUFtei1TaWduZWRIZWFkZXJzPWhvc3QmYWN0b3JfaWQ9MCZrZXlfaWQ9MCZyZXBvX2lkPTAifQ.SR6zvbDZpP70L0x6DL0X2AynT1Y6jME3gRz9IuK02JI">
  </a>
</p>

# PySyncNet
Python prototype for a real-time file system synchronizing system using HTML5 QUIC protocol.

## Motivation
I am pretty anal about backups and disaster recovery.  Right now, I'm swapping hardware in and out of a safe-deposit box at the bank, and have been for years now.  They yell my name when I walk in every Friday, just like Norm in "Cheers."  ;)

However, I'm envisioning something of a live system, stationed at a friend's or relative's house as a black-box setup where it just needs to be plugged in to power, internet, and then turned on.  The system in my house would then keep the live system synchronized in real time, with various features, like connection tolerance and resuming.

## State
I've been tinkering this together for some months now, and it's close to being ready for trial runs.  Once I've proven the system is reliable, I'll take the Python "blueprint" and convert it over to a native build (like C++ or Rust).

## WIP
This is entirely a WIP and hobby project, and is absolutely not in a production state.  This is my little brain child, and it will be a while yet before it's in any kind of useful state.