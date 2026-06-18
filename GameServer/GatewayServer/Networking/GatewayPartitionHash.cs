namespace GameServer.GatewayServer.Networking;

internal static class GatewayPartitionHash
{
    public static int ForZone(string zoneChannel, int partitionCount)
    {
        if (partitionCount <= 1)
        {
            return 0;
        }

        unchecked
        {
            var hash = 2166136261u;
            foreach (var ch in zoneChannel)
            {
                hash ^= ch;
                hash *= 16777619u;
            }

            return (int)(hash % (uint)partitionCount);
        }
    }

    public static int ForUser(long userDbId, int partitionCount)
    {
        if (partitionCount <= 1)
        {
            return 0;
        }

        return (int)((ulong)userDbId % (uint)partitionCount);
    }
}
