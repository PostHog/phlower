import { useRef } from "react";
import {
  type ColumnDef,
  type SortingState,
  flexRender,
  getCoreRowModel,
  getSortedRowModel,
  useReactTable,
} from "@tanstack/react-table";
import { useVirtualizer } from "@tanstack/react-virtual";
import { useState } from "react";

interface Props<T> {
  data: T[];
  columns: ColumnDef<T, unknown>[];
  getRowClassName?: (row: T) => string;
  virtualize?: boolean;
  estimateSize?: number;
  maxHeight?: number;
  initialSorting?: SortingState;
}

export function DataTable<T>({
  data,
  columns,
  getRowClassName,
  virtualize = false,
  estimateSize = 36,
  maxHeight = 600,
  initialSorting = [],
}: Props<T>) {
  const [sorting, setSorting] = useState<SortingState>(initialSorting);

  const table = useReactTable({
    data,
    columns,
    state: { sorting },
    onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
  });

  const { rows } = table.getRowModel();

  if (virtualize) {
    return (
      <VirtualizedBody
        table={table}
        rows={rows}
        getRowClassName={getRowClassName}
        estimateSize={estimateSize}
        maxHeight={maxHeight}
      />
    );
  }

  return (
    <table className="data-table">
      <thead>
        {table.getHeaderGroups().map((hg) => (
          <tr key={hg.id}>
            {hg.headers.map((header) => (
              <th
                key={header.id}
                className={header.column.columnDef.meta?.className ?? ""}
                onClick={header.column.getToggleSortingHandler()}
                style={{ cursor: header.column.getCanSort() ? "pointer" : "default" }}
              >
                {flexRender(header.column.columnDef.header, header.getContext())}
                {header.column.getIsSorted() === "asc"
                  ? " ↑"
                  : header.column.getIsSorted() === "desc"
                    ? " ↓"
                    : ""}
              </th>
            ))}
          </tr>
        ))}
      </thead>
      <tbody>
        {rows.map((row) => (
          <tr
            key={row.id}
            className={getRowClassName?.(row.original) ?? ""}
          >
            {row.getVisibleCells().map((cell) => (
              <td
                key={cell.id}
                className={cell.column.columnDef.meta?.className ?? ""}
              >
                {flexRender(cell.column.columnDef.cell, cell.getContext())}
              </td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  );
}

/** Virtualized table body — padding-based, normal table layout */
function VirtualizedBody<T>({
  table,
  rows,
  getRowClassName,
  estimateSize,
  maxHeight,
}: {
  table: ReturnType<typeof useReactTable<T>>;
  rows: ReturnType<ReturnType<typeof useReactTable<T>>["getRowModel"]>["rows"];
  getRowClassName?: (row: T) => string;
  estimateSize: number;
  maxHeight: number;
}) {
  const parentRef = useRef<HTMLDivElement>(null);

  const virtualizer = useVirtualizer({
    count: rows.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => estimateSize,
    overscan: 10,
  });

  const items = virtualizer.getVirtualItems();
  const totalSize = virtualizer.getTotalSize();
  const paddingTop = items.length > 0 ? items[0].start : 0;
  const paddingBottom = items.length > 0 ? totalSize - items[items.length - 1].end : 0;
  const colCount = table.getVisibleLeafColumns().length;

  return (
    <div
      ref={parentRef}
      className="virtual-table-container"
      style={{ maxHeight, overflow: "auto" }}
    >
      <table className="data-table">
        <thead>
          {table.getHeaderGroups().map((hg) => (
            <tr key={hg.id}>
              {hg.headers.map((header) => (
                <th
                  key={header.id}
                  className={header.column.columnDef.meta?.className ?? ""}
                  onClick={header.column.getToggleSortingHandler()}
                  style={{ cursor: header.column.getCanSort() ? "pointer" : "default" }}
                >
                  {flexRender(header.column.columnDef.header, header.getContext())}
                  {header.column.getIsSorted() === "asc"
                    ? " ↑"
                    : header.column.getIsSorted() === "desc"
                      ? " ↓"
                      : ""}
                </th>
              ))}
            </tr>
          ))}
        </thead>
        <tbody>
          {paddingTop > 0 && (
            <tr><td colSpan={colCount} style={{ height: paddingTop, padding: 0, border: "none" }} /></tr>
          )}
          {items.map((virtualRow) => {
            const row = rows[virtualRow.index];
            return (
              <tr
                key={row.id}
                className={getRowClassName?.(row.original) ?? ""}
              >
                {row.getVisibleCells().map((cell) => (
                  <td
                    key={cell.id}
                    className={cell.column.columnDef.meta?.className ?? ""}
                  >
                    {flexRender(cell.column.columnDef.cell, cell.getContext())}
                  </td>
                ))}
              </tr>
            );
          })}
          {paddingBottom > 0 && (
            <tr><td colSpan={colCount} style={{ height: paddingBottom, padding: 0, border: "none" }} /></tr>
          )}
        </tbody>
      </table>
    </div>
  );
}

// Extend column meta for custom className
declare module "@tanstack/react-table" {
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  interface ColumnMeta<TData, TValue> {
    className?: string;
  }
}
