using Microsoft.AspNetCore.SignalR;
using System.Collections.Concurrent;

namespace SignalRMvcApp.Hubs
{
    public class ChatHub : Hub
    {
        // قائمة لحفظ أسماء المستخدمين و ConnectionId الخاص بكل منهم
        private static readonly ConcurrentDictionary<string, string> ConnectedUsers = new();

        // عند اتصال مستخدم وتجميعه بالاسم
        public async Task RegisterUser(string username)
        {
            ConnectedUsers[username] = Context.ConnectionId;

            // إرسال تحديث بقائمة المستخدمين المتصلين للجميع
            await Clients.All.SendAsync("UpdateUserList", ConnectedUsers.Keys);
        }

        // إرسال رسالة خاصة لمستخدم معين
        public async Task SendPrivateMessage(string sender, string receiver, string message)
        {
            if (ConnectedUsers.TryGetValue(receiver, out var receiverConnectionId))
            {
                // إرسال الرسالة للمستهدف
                await Clients.Client(receiverConnectionId).SendAsync("ReceivePrivateMessage", sender, message);

                // إرسالها أيضاً للمرسل (ليراها في شاشته)
                await Clients.Caller.SendAsync("ReceivePrivateMessage", sender, message);
            }
        }

        // عند انقطاع الاتصال
        public override async Task OnDisconnectedAsync(Exception? exception)
        {
            var user = ConnectedUsers.FirstOrDefault(x => x.Value == Context.ConnectionId);
            if (!string.IsNullOrEmpty(user.Key))
            {
                ConnectedUsers.TryRemove(user.Key, out _);
                await Clients.All.SendAsync("UpdateUserList", ConnectedUsers.Keys);
                await Clients.All.SendAsync("UpdateUserList", ConnectedUsers.Keys ,);

            }
            await base.OnDisconnectedAsync(exception);
            await base.OnDisconnectedAsync(exception);
        }
       
    }
}