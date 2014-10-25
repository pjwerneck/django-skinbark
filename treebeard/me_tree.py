"""Matrix Encoding"""

__author__ = "Pedro Werneck (pjwerneck@gmail.com)"
__date__   = "Thu Apr 12 16:24:37 2012"

from copy import deepcopy

from django.db.models import Q, F
from django.db import models, transaction, connection

from treebeard.exceptions import InvalidMoveToDescendant
from treebeard.models import Node



def get_result_class(cls):
    """
    For the given model class, determine what class we should use for the
    nodes returned by its tree methods (such as get_children).

    Usually this will be trivially the same as the initial model class,
    but there are special cases when model inheritance is in use:

    * If the model extends another via multi-table inheritance, we need to
      use whichever ancestor originally implemented the tree behaviour (i.e.
      the one which defines the 'a11, a12, a21, a22' fields). We can't use the
      subclass, because it's not guaranteed that the other nodes reachable
      from the current one will be instances of the same subclass.

    * If the model is a proxy model, the returned nodes should also use
      the proxy class.
    """
    base_class = cls._meta.get_field('matrix').model
    if cls._meta.proxy_for_model == base_class:
        return cls
    else:
        return base_class



def _dot(a, b):
    return (a[0]*b[0] + a[1]*b[2], a[0]*b[1] + a[1]*b[3],
            a[2]*b[0] + a[3]*b[2], a[2]*b[1] + a[3]*b[3])


class ME_Node(Node):
    """Abstract model implementing Matrix Encoded Nested Sets Trees

    """
    parent = models.ForeignKey('self', null=True, blank=True)
    
    a11 = models.BigIntegerField(db_index=True)
    a12 = models.BigIntegerField(db_index=True)
    a21 = models.BigIntegerField(db_index=True)
    a22 = models.BigIntegerField(db_index=True)

    tree_id = models.PositiveIntegerField(db_index=True)
    
    class Meta:
        """Abstract model."""
        abstract = True

    def __unicode__(self):
        return u'%s, %s'%(self.name, self.me)

    index = property()    
    
    @index.getter
    def index(self):
        return int(self.a11/self.a12)

    @index.setter
    def index(self, index):
        # recreate matrix encoding for this node and index position
        tmp = (index+1, -1, 1, 0)
        if self.parent is None:
            me = tmp
        else:
            me = _dot(self.parent.me, tmp)

        # sanity check
        assert me[0] * me[3] - me[2] * me[1] == 1
        self.me = me
        
    me = property()
    @me.getter
    def me(self):
        return (self.a11, -self.a12, self.a21, -self.a22)

    @me.setter
    def me(self, m):
        self.a11 = abs(m[0])
        self.a12 = abs(m[1])
        self.a21 = abs(m[2])
        self.a22 = abs(m[3])

    @classmethod
    def add_root(cls, **kwargs):

        last_root = cls.get_last_root_node()

        if last_root:
            newroot_id = last_root.tree_id + 1
        else:
            newroot_id = 1
            
        newroot = cls(tree_id=newroot_id, **kwargs)
        newroot.index = 0
        newroot.save()
        transaction.commit_unless_managed()
        return newroot

    @classmethod
    def get_root_nodes(cls):
        return cls.objects.filter(parent__isnull=True)

    @classmethod
    def get_last_root_node(cls):
        try:
            return cls.get_root_nodes().reverse()[0]
        except IndexError:
            return None

    def load_data(self, data):
        data = deepcopy(data)
        i = 1
        for n in data:
            children = n.pop('children', [])
            node = self.add_child(**n)
            if children:
                node.load_data(children)
          
    def add_child(self, **kwargs):
        # get last child's position at this level
        i = self._sql_get_last_child()

        # new node at i+1
        new = self.__class__(parent=self, tree_id=self.tree_id, index=i+1, **kwargs)
        new.save()
        transaction.commit_unless_managed()
        return new
        
    def _sql_get_last_child(self):
        sql = "SELECT MAX(a11/a12) "\
              "FROM %(table)s "\
              "WHERE a12 = %(a11)s AND a22 = %(a21)s"

        sql = sql % {'table':self._meta.db_table,
                     'a11':self.a11,
                     'a21':self.a21,
                     }
        cursor = connection.cursor()
        cursor.execute(sql)
        i = cursor.fetchone()[0]
        cursor.close()

        if i is None:
            i = 0

        return i

    def _get_parent(self):
        if self.a11 == self.a21 == 1:
            # root node
            return None

        return self.__class__.objects.get(a11=self.a12, a21=self.a22)

    @property
    def level(self):
        # is there any better way to do this?
        return self._sql_get_level()

    def _sql_get_level(self):
        sql = "SELECT COUNT(id) FROM %(table)s "\
              "WHERE (%(a11)s-%(a12)s) * (a21-a22) >= (a11-a12) * (%(a21)s-%(a22)s) "\
              "AND (a11*%(a21)s) >= (%(a11)s*a21) "\
              "AND id != %(id)s"

        sql = sql % {'table':self._meta.db_table,
                     'id': self.pk,
                     'a11':self.a11,
                     'a12':self.a12,
                     'a21':self.a21,
                     'a22':self.a22,
                     }
        cursor = connection.cursor()
        cursor.execute(sql)
        i = cursor.fetchone()[0]
        cursor.close()

        if i is None:
            i = 0

        return i
    
    def get_children(self):
        return self.__class__.objects.filter(a12=self.a11, a22=self.a21)

    def get_children_count(self):
        return self.get_children().count()

    def get_siblings(self):
        return self.__class__.objects.filter(a12=self.a12, a22=self.a22).exclude(a11=self.a11, a21=self.a21)

    def get_next_sibling(self):
        try:
            return self.__class__.objects.filter(a12=self.a12, a22=self.a22, a11__gt=self.a11, a21__gt=self.a21).order_by('a11', 'a21')[0]
        except IndexError:
            return None
        
    def get_prev_sibling(self):
        try:
            return self.__class__.objects.filter(a12=self.a12, a22=self.a22, a11__lt=self.a11, a21__lt=self.a21).order_by('a11', 'a21').reverse()[0]
        except IndexError:
            return None
        

    def get_ancestors(self, order_by=None):
        return self._sql_get_ancestors(order_by=None)

    def get_descendants(self, order_by=None):
        return self._sql_get_descendants(order_by=None)
    
    def _sql_get_ancestors(self, **kwargs):
        sql = "SELECT * FROM %(table)s "\
              "WHERE (%(a11)s-%(a12)s) * (a21-a22) >= (a11-a12) * (%(a21)s-%(a22)s) "\
              "AND (a11*%(a21)s) >= (%(a11)s*a21) "\
              "AND id != %(id)s"

        return self._sql_get_ancestry(sql, **kwargs)
        
    def _sql_get_descendants(self, **kwargs):
        sql = "SELECT * FROM %(table)s "\
              "WHERE (%(a11)s-%(a12)s) * (a21-a22) <= (a11-a12) * (%(a21)s-%(a22)s) "\
              "AND (a11*%(a21)s) <= (%(a11)s*a21) "\
              "AND id != %(id)s"

        return self._sql_get_ancestry(sql, **kwargs)
        
    def _sql_get_ancestry(self, sql, order_by=None):

        if order_by is not None:
            sql += "ORDER BY %(order)s"

        sql = sql % {'table':self._meta.db_table,
                     'id':self.pk,
                     'a11':self.a11,
                     'a12':self.a12,
                     'a21':self.a21,
                     'a22':self.a22,
                     'order':order_by,
                     }
        query = self.__class__.objects.raw(sql)
        return query        

    def get_path(self):
        pass
    
