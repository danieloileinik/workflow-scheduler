using Microsoft.EntityFrameworkCore;
using OrderDemo.Contracts;

namespace OrderDemo.Inventory;

public sealed class InventoryDb(DbContextOptions<InventoryDb> options) : DbContext(options)
{
    public DbSet<InventoryOrder> Orders => Set<InventoryOrder>();
    public DbSet<OperationReceipt> Executions => Set<OperationReceipt>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<InventoryOrder>(order =>
        {
            order.ToTable("inventory_orders");
            order.HasKey(value => value.Id);
            order.Property(value => value.Id).ValueGeneratedNever();
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
