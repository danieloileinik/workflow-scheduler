using Microsoft.EntityFrameworkCore;
using OrderDemo.Contracts;

namespace OrderDemo.Payments;

public sealed class PaymentDb(DbContextOptions<PaymentDb> options) : DbContext(options)
{
    public DbSet<PaymentOrder> Orders => Set<PaymentOrder>();
    public DbSet<OperationReceipt> Executions => Set<OperationReceipt>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<PaymentOrder>(order =>
        {
            order.ToTable("payment_orders");
            order.HasKey(value => value.Id);
            order.Property(value => value.Id).ValueGeneratedNever();
            order.Property(value => value.Amount).HasPrecision(12, 2);
            order.Property(value => value.State).HasMaxLength(32);
        });
        modelBuilder.Entity<OperationReceipt>(receipt =>
        {
            receipt.ToTable("executions");
            receipt.HasKey(value => value.ExecutionKey);
            receipt.Property(value => value.ExecutionKey).HasMaxLength(64);
            receipt.Property(value => value.Command).HasMaxLength(64);
            receipt.Property(value => value.Detail).HasMaxLength(512);
            receipt.HasIndex(value => new { value.OrderId, value.RecordedAt });
        });
    }
}
